;; Copyright (c) 2008,2009 Lau B. Jensen <lau.jensen {at} bestinclass.dk
;;                         Meikel Brandmeyer <mb {at} kotka.de>
;; All rights reserved.
;;
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file LICENSE.txt at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by the
;; terms of this license. You must not remove this notice, or any other, from
;; this software.

(clojure.core/in-ns 'dk.bestinclass.clojureql)

;; DEFINITIONS =============================================

(defn- trace*
  [qx x]
  (print "TRACE: ") (pr qx) (print " = ") (prn x) (flush)
  x)

(defmacro #^{:private true} trace
  [x]
  `(trace* (quote ~x) ~x))

; Queries

(defstruct sql-query
  :type :columns :tables :predicates :column-aliases :table-aliases :env)

(defstruct sql-join
  :type :query :env)

(defstruct sql-ordered-query
  :type :query :order :columns :env)

(defstruct sql-grouped-query
  :type :query :columns :env)

(defstruct sql-having-query
  :type :query :env)

(defstruct sql-distinct-query
  :type :query :env)

(defstruct sql-union
  :type :all :queries :env)

(defstruct sql-intersect
  :type :queries :env)

(defstruct sql-difference
  :type :queries :env)

(defstruct sql-let-query
  :type :fn)

; Data Handling

(defstruct sql-insert
  :type :table :columns :env)

(defstruct sql-update
  :type :table :columns :predicates :env)

(defstruct sql-delete
  :type :table :predicates :env)

; Table Handling

(defstruct sql-create-table
  :type :table :columns :primary)

(defstruct sql-drop-table
  :type :table :if-exists)

(defstruct sql-alter-table
  :type :table :action)

; Special Statements

(defstruct sql-batch-statement
  :type :statements)

(defstruct sql-raw-statement
  :type :statement)

;; HIERARCHY ===============================================

(def
  #^{:private true
     :doc
  "Hierarchy for the SELECT statements."}
  select-hierarchy
  (-> (make-hierarchy)
    (derive ::Join           ::Select)
    (derive ::InnerJoin      ::Join)
    (derive ::LeftJoin       ::Join)
    (derive ::RightJoin      ::Join)
    (derive ::FullJoin       ::Join)
    (derive ::OrderedSelect  ::Select)
    (derive ::GroupedSelect  ::Select)
    (derive ::DistinctSelect ::Select)))

(defn- is-and-not?
  "Checks whether the given query is of the type isa (or any derivee).
  If so, checks also any subquery. Returns nil or the offending query
  type."
  [kwery isa is-not-a]
  (if (and      (isa? select-hierarchy (kwery :type) isa)
           (not (isa? select-hierarchy (kwery :type) is-not-a)))
    (when-let [sub-query (kwery :query)]
      (is-and-not? sub-query isa is-not-a))
    (kwery :type)))

;; COMPILER ================================================

(def #^{:doc "A map of functions to their type."} where-clause-type
  (atom {"and" ::Recursive
         "or"  ::Recursive
         "not" ::Recursive}))

(defmulti build-env
  "Build environment vector. Replace extracted values with ?."
  {:arglists '([form env])}
  (fn build-env-dispatch [form _]
    (when-let [form (seq form)]
      (get @where-clause-type (-> form first ->string) ::NonRecursive))))

(defmethod build-env ::Recursive
  build-env-recursive
  [[function & forms] env]
  (let [[forms env]
        (reduce
          (fn [[forms env] form]
            (let [[form env] (build-env form env)]
              [(conj forms form) env]))
          [[] env] forms)]
    [(vec (cons function forms)) env]))

(defmethod build-env ::NonRecursive
  build-env-non-recursive
  [[function & args] env]
  (let [[args env]
        (reduce (fn [[args env] arg]
                  (cond
                    (nil? arg)       [(conj args "NULL") env]
                    (self-eval? arg) [(conj args "?")    (conj env arg)]
                    :else            [(conj args arg)    env]))
                [[] env] args)]
    [(vec (cons function args)) env]))

(defmethod build-env nil
  build-env-nil
  [_ _]
  nil)

; Queries

(defn raw
  "Executes a raw SQL statement - This should not be necessary and its
   definately not recommended. If you find ClojureQL lacking in features
   please leave us a note on GitHub!"
  [txt]
  (struct-map sql-raw-statement
    :type      ::Raw
    :statement txt))

(defn query*
  "Driver for the query macro. Don't call directly!"
  [col-spec table-spec pred-spec]
  (let [col-spec   (->vector col-spec)
        col-spec   (mapcat fix-prefix col-spec)
        col-spec   (map ->vector col-spec)
        [col-spec col-aliases]     (reduce check-alias [[] {}] col-spec)

        table-spec (->vector table-spec)
        table-spec (map ->vector table-spec)
        [table-spec table-aliases] (reduce check-alias [[] {}] table-spec)

        [pred-spec env]            (build-env pred-spec [])]
    (struct-map sql-query
                :type           ::Select
                :columns        col-spec
                :tables         table-spec
                :predicates     pred-spec
                :column-aliases col-aliases
                :table-aliases  table-aliases
                :env            env)))

(defmacro query
  "Define a SELECT query."
  ([col-spec table-spec]
   `(query ~col-spec ~table-spec nil))
  ([col-spec table-spec pred-spec]
   `(query* ~@(map quasiquote* [col-spec table-spec pred-spec]))))

(defn join
  "Turn a query into JOIN."
  [join-type kwery]
  (let [join-types {:inner ::InnerJoin
                    :left  ::LeftJoin
                    :right ::RightJoin
                    :full  ::FullJoin}]
    (struct-map sql-join
                :type  (get join-types join-type ::InnerJoin)
                :query kwery
                :env   (kwery :env))))

(defn order-by*
  "Driver for the order-by macro. Don't call directly."
  [kwery & columns]
  (when-let [offender (is-and-not? kwery ::Select ::OrderedSelect)]
    (throw (Exception. (str "Unexpected query type: " offender))))
  (let [order   (first columns)
        columns (vec (if (keyword? order) (drop 1 columns) columns))
        order   (if (keyword? order) order :ascending)]
    (struct-map sql-ordered-query
                :type    ::OrderedSelect
                :query   kwery
                :order   order
                :columns columns
                :env     (kwery :env))))

(defmacro order-by
  "Modify the given query to be order according to the given columns. The first
  argument may be one of the keywords :ascending or :descending to choose the
  order used."
  [kwery & columns]
  `(order-by* ~kwery ~@(map quasiquote* columns)))

(defn group-by*
  [kwery & columns]
  (when-let [offender (is-and-not? kwery ::Select ::GroupedSelect)]
    (throw (Exception. (str "Unexpected query type: " offender))))
  (let [columns (vec columns)]
    (struct-map sql-grouped-query
                :type    ::GroupedSelect
                :query   kwery
                :columns columns
                :env     (kwery :env))))

(defmacro group-by
  "Modify the given query to be group by the given columns."
  [kwery & columns]
  `(group-by* ~kwery ~@(map quasiquote* columns)))

(defn having*
  "Driver for the having macro. Should not be called directly."
  [kwery pred-spec]
  (when-let [offender (is-and-not? kwery ::Select ::HavingSelect)]
    (throw (Exception. (str "Unexpected query type: " offender))))
  (let [[pred-spec env] (build-env pred-spec (kwery :env))]
    (struct-map sql-having-query
                :type       ::HavingSelect
                :query      kwery
                :predicates pred-spec
                :env        env)))

(defmacro having
  "Add a HAVING clause to the given query."
  [kwery pred-spec]
  `(having* ~kwery ~(quasiquote* pred-spec)))

(defn distinct!
  "Modify the given query to return only distinct results."
  [kwery]
  (when-let [offender (is-and-not? kwery ::Select ::DistinctSelect)]
    (throw (Exception. (str "Unexpected query type: " offender))))
  (struct-map sql-distinct-query
              :type  ::DistinctSelect
              :query kwery
              :env   (kwery :env)))

(defn union
  "Build the union of the given queries. The first argument may be the keyword
  :all in order to include all results in the union. Without :all only distinct
  results are included."
  [& kweries]
  (condp = (count kweries)
    0 nil
    1 (first kweries)
    (let [all     (= (first kweries) :all)
          kweries (vec (if all (drop 1 kweries) kweries))]
      (struct-map sql-union
                  :type    ::Union
                  :all     all
                  :queries kweries
                  :env     (vec (mapcat :env kweries))))))

(defn intersect
  "Build the intersection of the given queries."
  [& kweries]
  (condp = (count kweries)
    0 nil
    1 (first kweries)
    (struct-map sql-intersect
                :type    ::Intersect
                :queries kweries
                :env     (vec (mapcat :env kweries)))))

(defn difference
  "Build the difference of the given queries."
  [& kweries]
  (condp = (count kweries)
    0 nil
    1 (first kweries)
    (struct-map sql-difference
                :type    ::Difference
                :queries kweries
                :env     (vec (mapcat :env kweries)))))

(declare execute-sql)

(defmacro let-query
  "Takes a let-style binding vector and returns a new query, which,
  when executed, assigns the queries results to the named locals and
  executes the body. The result of the body is returned as the query's
  result."
  [bindings & body]
  (let [conn    (gensym "let_query_conn__")
        locals  (take-nth 2 bindings)
        kweries (take-nth 2 (rest bindings))]
    `(struct-map sql-let-query
                 :type ::LetQuery
                 :fn   (fn [~conn]
                         (let ~(vec (interleave
                                      locals
                                      (map (fn [kwery]
                                             `(execute-sql ~kwery ~conn))
                                           kweries)))
                           ~@body)))))

; Data Handling

(defn insert-into*
  "Driver for the insert-into macro. Don't use directly."
  [table & col-val-pairs]
  (if (even? (count col-val-pairs))
    (let [columns (take-nth 2 col-val-pairs)
          values  (take-nth 2 (rest col-val-pairs))]
      (struct-map sql-insert
                  :type    ::Insert
                  :table   table
                  :columns columns
                  :env     values))
    (throw (Exception. "column/value pairs not balanced"))))

(defmacro insert-into
  "Insert data into a table."
  [table & col-val-pairs]
  `(insert-into* ~@(map quasiquote* (cons table col-val-pairs))))

(defn update*
  "Driver for the update macro. Don't call directly."
  [table col-val-pairs pred-spec]
  (if (even? (count col-val-pairs))
    (let [columns         (vec (take-nth 2 col-val-pairs))
          values          (vec (take-nth 2 (rest col-val-pairs)))
          [pred-spec env] (build-env pred-spec values)]
      (struct-map sql-update
                  :type       ::Update
                  :table      table
                  :columns    columns
                  :predicates pred-spec
                  :env        env))
    (throw (Exception. "column/value pairs not balanced"))))

(defmacro update
  "Update the given columns of the given table with the associated values
  where the predicates are satisfied. The relation between columns and
  values is given as a let-style binding vector."
  [table col-val-pairs pred-spec]
  `(update* ~@(map quasiquote* [table col-val-pairs pred-spec])))

(defn delete-from*
  "Driver for the delete-from macro. Don't call directly."
  [table pred-spec]
  (let [[pred-spec env] (build-env pred-spec [])]
    (struct-map sql-delete
                :type       ::Delete
                :table      table
                :predicates pred-spec
                :env        env)))

(defmacro delete-from
  "Delete the entries matching the given predicates from the given table."
  [table pred-spec]
  `(delete-from* ~@(map quasiquote* [table pred-spec])))

; Table Handling

(defmulti alter-table*
  (fn [table & options] (first options)))

(defmethod alter-table* 'add
  [table & options]
  (let [action     (first options)
        keycoll    (last options)
        options    (butlast (rest options))]
    (struct-map sql-alter-table
                :type    ::AlterTable
                :subtype ::Add
                :table   table
                :action  action
                :options options
                :keycoll keycoll)))

(defmethod alter-table* 'change
  [table & options]
  (let [action     (first options)
        keycoll    (last options)
        options    (butlast (rest options))]
    (struct-map sql-alter-table
                :type    ::AlterTable
                :subtype ::Change
                :table   table
                :action  action
                :options options
                :keycoll keycoll)))

(defmethod alter-table* 'modify
  [table & options]
  (let [col-name  (first (rest options))
        new-type  (last  (rest options))]
    (struct-map sql-alter-table
                :type     ::AlterTable
                :subtype  ::Modify
                :table    table
                :column   col-name
                :new-type new-type)))

(defmethod alter-table* 'drop
  [table & options]
  (if (= options '(drop primary key))
    (struct-map sql-alter-table
                :type    ::AlterTable
                :subtype :DropPrimary
                :table   table)
    (let [target-type (butlast (rest options))
          target      (last    (rest options))]
      (struct-map sql-alter-table
                  :type        ::AlterTable
                  :subtype     ::Drop
                  :table       table
                  :target-type target-type
                  :target      target))))

(defmacro alter-table
  [table & options]
  `(alter-table* ~@(map quasiquote* (list* table options))))

;;;==== EXPERIMENTAL SUBSTITUTIONS FOR ALTER

(defmacro set-primary
  [table id]
  `(alter-table ~table ~'add ~'primary ~'key ~id))

(defmacro set-notnull
  [table fields]
  (let [ [field type-of-field] fields ]
  `(alter-table ~table ~'change ~field ~field ~type-of-field ~'NOT ~'NULL)))

(defmacro drop-primary
  [table]
  `(alter-table ~table ~'drop ~'primary ~'key))

(defmacro drop-table
  [table]
  `(alter-table ~table ~'drop))

;;;==== EXPERIMENTAL SUBSTITUTIONS FOR ALTER

(declare batch-statements)

(defn create-table*
  "Driver function for create-table macro. Don't use directly."
  [table columns & options]
  (let [columns (apply array-map (->vector columns))
        options (merge {:primary-key nil
                        :non-nulls   []
                        :auto-inc    []}
                       (apply hash-map options))]
    (struct-map sql-create-table
                :type    ::CreateTable
                :table   table
                :columns columns
                :options options)))

(defmacro create-table
  "Create a table of the given name and the given columns.

   ex.
    (create-table foo [id 'int(11)' name 'varchar(100)' lifestory 'text']
                      :primary id :not-null id :auto-inc id"
  [table & columns]
  `(create-table* ~@(map quasiquote* (cons table columns))))

(defn drop-table*
  "Driver function for the drop-table macro. Don't use directly."
  [table & if-exists]
  (let [if-exists (= (first if-exists) :if-exists)]
    (struct-map sql-drop-table
                :type      ::DropTable
                :table     table
                :if-exists if-exists)))

(defmacro drop-table
  "Drop the given table. Optionally :if-exists might be specified."
  [table & if-exists]
  `(drop-table* ~@(map quasiquote* (cons table if-exists))))

(defn batch-statements
  "Execute the given statements in a batch wrapped in a dedicated
  transaction."
  [& statements]
  (struct-map sql-batch-statement
              :type       ::Batch
              :statements statements))

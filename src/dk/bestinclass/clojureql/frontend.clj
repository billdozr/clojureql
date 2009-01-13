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

; Queries

(defstruct sql-query
  :type :columns :tables :predicates :column-aliases :table-aliases :env :sql)

(defstruct sql-ordered-query
  :type :query :order :columns :env :sql)

(defstruct sql-grouped-query
  :type :query :columns :env :sql)

(defstruct sql-having-query
  :type :query :env :sql)

(defstruct sql-distinct-query
  :type :query :env :sql)

(defstruct sql-union
  :type :all :queries :env :sql)

(defstruct sql-intersect
  :type :queries :env :sql)

(defstruct sql-difference
  :type :queries :env :sql)

(defstruct sql-let-query
  :type :fn)

; Data Handling

(defstruct sql-insert
  :type :table :columns :env :sql)

(defstruct sql-update
  :type :table :columns :predicates :env :sql)

(defstruct sql-delete
  :type :table :predicates :env :sql)

; Table Handling

(defstruct sql-create-table
  :type :table :columns :primary :sql)

(defstruct sql-drop-table
  :type :table :if-exists :sql)

(defstruct sql-alter-table
  :type :table :action :sql)

; Special Statements

(defstruct sql-batch-statement
  :type :statements)

;; HIERARCHY ===============================================

(def
  #^{:private true
     :doc
  "Hierarchy for the SELECT statements."}
  select-hierarchy
  (-> (make-hierarchy)
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

;; HELPERS =================================================

(defn- ->vector
  "Takes 1 argument and converts it into a vector"
  [thing]
  (cond
    (vector? thing)  thing
    (list? thing)    (vector thing)
    (string? thing)  (vector thing)
    (symbol? thing)  (vector thing)
    (keyword? thing) (vector thing)
    :else (throw (Exception.
                   "only Symbols, Keywords, Strings or Vectors are allowed"))))

(defn- ->string
  "Converts the given thing to a string."
  [thing]
  (cond
    (string? thing)  thing
    (symbol? thing)  (name thing)
    (keyword? thing) (name thing)
    :else            (str thing)))

(defn- column-from-spec
  "Try to get the column from a specificaton. Examples:
     (column-from-spec col) => col
     (column-from-spec (??? col ...) => col
     (column-from-spec [col :as ...]) => col
     (column-from-spec [(??? col ...) :as ...]) => col"
  [col-spec]
  (cond
    (vector? col-spec) (column-from-spec (first col-spec))
    (list? col-spec)   (second col-spec)
    :else              col-spec))

(defn- table-from-spec
  "Try to get the table from a specificaton. Examples:
     (table-from-spec table) => table
     (table-from-spec [table :as ???]) => table"
  [table-spec]
  (cond
    (vector? table-spec) (first table-spec)
    :else                table-spec))

(defn- check-alias
  [[specs aliases] [orig as aka]]
  (if (= as :as)
    (vector (conj specs orig) (conj aliases [(column-from-spec orig) aka]))
    (vector (conj specs orig) aliases)))

(defn- fix-prefix
  "Takes a prefix and a series of columns and prepends the prefix to
  every column.

  Example:
    (fix-prefix '[table1 :cols a b c]) => (table1.a table1.b table1.c)"
  [col-spec]
  (if (vector? col-spec)
    (let [[prefix prefix-for & cols] col-spec]
      (if (= prefix-for :cols)
        (let [str-prefix   (->string prefix)
              resolve-spec (fn resolve-spec [col]
                             (if (list? col)
                               (let [[function col & rst] col]
                                 (list* function (resolve-spec col) rst))
                               (symbol (str str-prefix "." (->string col)))))]
          (map (fn [col]
                 (if (vector? col)
                   (vec (cons (resolve-spec (first col)) (rest col)))
                   (resolve-spec col)))
               cols))
        [col-spec]))
    [col-spec]))

(defn- self-eval?
  "Check whether the given form is self-evaluating."
  [thing]
  (or (keyword? thing)
      (number? thing)
      (instance? Character thing)
      (string? thing)))

(defn- flatten-map
  "Flatten the keys and values of a map into a list."
  [the-map]
  (reduce (fn [result entry] (-> result
                               (conj (key entry))
                               (conj (val entry))))
          [] the-map))

(defn- unquote?
  "Tests whether the given form is of the form (unquote ...)."
  [form]
  (and (seq? form) (= (first form) `unquote)))

(defn- quasiquote*
  "Worker for quasiquote macro. See docstring there. For use in macros."
  [form]
  (cond
    (self-eval? form) form
    (unquote? form)   (second form)
    (symbol? form)    (list 'quote form)
    (vector? form)    (vec (map quasiquote* form))
    (map? form)       (apply hash-map (map quasiquote* (flatten-map form)))
    (set? form)       (apply hash-set (map quasiquote* form))
    (seq? form)       (list* `list (map quasiquote* form))
    :else             (list 'quote form)))

(defmacro quasiquote
  "Quote the supplied form as quote does, but evaluate unquoted parts.

  Example: (let [x 5] (quasiquote (+ ~x 6))) => (+ 5 6)"
  [form]
  (quasiquote* form))

(defn str-cat
  "Concate collection to a string. The member a separated by separator."
  [sep coll]
  (apply str (interpose sep coll)))

;; COMPILER ================================================

(defn pa
  " pa=Print AST, helper func for debugging purposes "
  [ast]
  (doseq [entry ast]
    (prn entry)))

(defn build-env
  "Build environment vector. Replace extracted values with ?."
  [[function col value & more :as form] env]
  (let [and-or?    (comp #{"or" "and"} ->string)
        predicate? (comp #{"=" "<=" ">=" "<" ">" "<>" "like"} ->string)]
    (when function
      (cond
        (and-or? function)    (let [[rst env]
                                    (reduce
                                      (fn [[rst env] more]
                                        (let [[more env] (build-env more env)]
                                          [(conj rst more) env]))
                                      [[] env]
                                      (rest form))]
                                [(vec (cons function rst)) env])
        (predicate? function) (if (self-eval? value)
                                [[function col "?"] (conj env value)]
                                [[function col value] env])
        :else (throw (Exception.
                       (str "Unsupported predicate form: " function)))))))

(defn compile-alias
  "Checks whether the given column has an alias in the aliases map. If so
  it is converted to a SQL alias of the form „column AS alias“."
  [col-or-table-spec col-or-table aliases]
  (if-let [aka (aliases col-or-table)]
    (str (->string col-or-table-spec) " AS " (->string aka))
    col-or-table-spec))

(defn- sql-function-type
  "Returns the type of the given SQL function. This is basically only
  interesting to catch the mathematical operators, which are infixed.
  Other function calls are handled normally."
  [sql-fun]
  (let [infix? (comp #{"+" "-" "*" "/" "and" "or" "="
                       "<=" ">=" "<" ">" "<>" "like"}
                     ->string)]
    (if (infix? sql-fun)
      :infix
      :funcall)))

(declare compile-function)

(defn infixed
  [form]
  (str "(" (str-cat " " (interpose (->string (first form))
                                   (map compile-function (rest form))))
       ")"))

(defn compile-function
  [col-spec]
  "Compile a function specification into a string."
  (if (or (list? col-spec) (vector? col-spec))
    (let [[function col & args] col-spec]
      (if (= (sql-function-type function) :infix)
        (infixed col-spec)
        (str function "(" (str-cat "," (cons (->string col) args)) ")")))
    col-spec))

;; AST-BUILDING ============================================

; Queries

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
                :env            env
                :sql
                (let [cols   (str-cat ","
                               (map (fn [spec]
                                      (let [col (column-from-spec spec)]
                                        (-> spec
                                          compile-function
                                          (compile-alias col col-aliases)
                                          ->string)))
                                    col-spec))
                      tables (str-cat ","
                               (map (fn [spec]
                                      (let [table (table-from-spec spec)]
                                        (-> spec
                                          (compile-alias table table-aliases)
                                          ->string)))
                                    table-spec))
                      stmnt  (list* "SELECT" cols
                                    "FROM"   tables
                                    (when pred-spec
                                      (list "WHERE" (infixed pred-spec))))]
                  (str-cat " " stmnt)))))

(defmacro query
  "Define a SELECT query."
  ([col-spec table-spec]
   `(query ~col-spec ~table-spec nil))
  ([col-spec table-spec pred-spec]
   `(query* ~@(map quasiquote* [col-spec table-spec pred-spec]))))

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
                :env     (kwery :env)
                :sql     (str-cat " " [(kwery :sql)
                                       "ORDER BY"
                                       (str-cat "," (map ->string columns))
                                       (condp = order
                                         :ascending  "ASC"
                                         :descending "DESC")]))))

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
                :env     (kwery :env)
                :sql     (str-cat " " [(kwery :sql)
                                       "GROUP BY"
                                       (str-cat "," (map ->string columns))]))))

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
                :type     ::HavingSelect
                :query    kwery
                :env      env
                :sql      (str-cat " " [(kwery :sql)
                                        "HAVING"
                                        (compile-function pred-spec)]))))

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
              :env   (kwery :env)
              :sql   (apply str "SELECT DISTINCT" (drop 6 (kwery :sql)))))

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
                  :env     (vec (mapcat :env kweries))
                  :sql     (str "("
                                (str-cat " " (interpose (if all
                                                          ") UNION ALL ("
                                                          ") UNION (")
                                                        (map :sql kweries)))
                                ")")))))

(defn intersect
  "Build the intersection of the given queries."
  [& kweries]
  (condp = (count kweries)
    0 nil
    1 (first kweries)
    (struct-map sql-intersect
                :type    ::Intersect
                :queries kweries
                :env     (vec (mapcat :env kweries))
                :sql     (str "("
                              (str-cat " " (interpose ") INTERSECT ("
                                                      (map :sql kweries)))
                              ")"))))

(defn difference
  "Build the difference of the given queries."
  [& kweries]
  (condp = (count kweries)
    0 nil
    1 (first kweries)
    (struct-map sql-difference
                :type    ::Difference
                :queries kweries
                :env     (vec (mapcat :env kweries))
                :sql     (str "("
                              (str-cat " " (interpose ") MINUS ("
                                                      (map :sql kweries)))
                              ")"))))

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
                  :env     values
                  :sql
                  (str-cat " " ["INSERT INTO" table "("
                                (str-cat "," (map ->string columns))
                                ") VALUES ("
                                (str-cat "," (take (count columns) (repeat "?")))
                                ")"])))
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
                  :env        env
                  :sql
                  (str-cat " " ["UPDATE" table
                                "SET"    (str-cat "," (map (comp
                                                             #(str % " = ?")
                                                             ->string)
                                                           columns))
                                "WHERE"  (infixed pred-spec)])))
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
                :env        env
                :sql
                (str-cat " " ["DELETE FROM" table
                              "WHERE"       (infixed pred-spec)]))))

(defmacro delete-from
  "Delete the entries matching the given predicates from the given table."
  [table pred-spec]
  `(delete-from* ~@(map quasiquote* [table pred-spec])))

; Table Handling

(defn alter-table*
  [table & options]
  (let [action     (first options)
        keycoll    (last options)
        options    (butlast (rest options))]
  (struct-map sql-alter-table
    :type       ::AlterTable
    :table      table
    :action     action
    :sql        (str-cat " " (list "ALTER TABLE"
                                   table
                                   action
                                   (str-cat " " options)
                                   (if (= 'add action)
                                     (str "(" keycoll ")" )
                                     keycoll))))))
    

(defmacro alter-table
  [table & options]
  `(alter-table* ~@(map quasiquote* (list* table options))))

(defn create-table*
  "Driver function for create-table macro. Don't use directly."
  [table column-vec & options]
  (let [columns    (->vector column-vec)
        options    (apply hash-map options)
        primary    (:primary options)
        flat-map   (fn flat-map [coll]
                     (if (list? coll)
                       (map flat-map coll)
                       (apply str (str-cat " " coll))))
        create-ast (struct-map sql-create-table
                     :type    ::CreateTable
                     :table   table
                     :columns columns
                     :primary (:primary options)
                     :sql
                     (let [cols     (apply str (map #(str (flat-map %))
                                                    (interpose "," (partition 2 columns))))]
                       (str-cat " " (list* "CREATE TABLE" table "(" cols ")"))))]
    (if (nil? primary)
      create-ast
      (let [column-vec  (apply array-map column-vec)]
        (struct-map sql-batch-statement
          :type       ::Batch
          :statements [create-ast
                       (when (:primary options)
                         (alter-table ~table add primary key ~primary))
                       (when (:not-null options)
                         (let [not-null      (:not-null options)
                               not-null-type (not-null column-vec)]
                           (alter-table ~table change ~not-null ~not-null ~not-null-type NOT NULL)))
                       (when (:auto-inc options)
                         (let [auto-inc      (:auto-inc options)
                               auto-inc-type ((:auto-inc options) column-vec)]
                           (alter-table ~table change ~auto-inc ~auto-inc ~auto-inc-type  AUTO_INCREMENT)))])))))
                        


        
      

(defmacro create-table
  "Create a table of the given name and the given columns."
  [table & columns]
  `(create-table* ~@(map quasiquote* (cons table columns))))



(defn drop-table*
  "Driver function for the drop-table macro. Don't use directly."
  [table & if-exists]
  (let [if-exists (= (first if-exists) :if-exists)]
    (struct-map sql-drop-table
                :type      ::DropTable
                :tabel     table
                :if-exists if-exists
                :sql
                (str-cat " " ["DROP TABLE"
                              (when if-exists
                                "IF EXISTS")
                              table]))))

(defmacro drop-table
  "Drop the given table. Optionally :if-exists might be specified."
  [table & if-exists]
  `(drop-table* ~@(map quasiquote* (cons table if-exists))))

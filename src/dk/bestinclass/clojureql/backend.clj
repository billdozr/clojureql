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

;; CONNECTION ==============================================

(defstruct connection-info
  :jdbc-url
  :username
  :password)

(defn make-connection-info
  ([protocol host username password]
     (struct connection-info (format "jdbc:%s:%s" protocol host) username password))
  ([protocol host]
     (struct connection-info (format "jdbc:%s:%s" protocol host) nil nil)))

;; MACROS ==================================================

(defn set-env
  [stmt env]
  (when (pos? (count env))
    (loop [env env
           cnt 1]
      (when env
        (let [value (first env)]
          (condp instance? value
            String             (.setString    stmt cnt value)
            Float              (.setFloat     stmt cnt value)
            Double             (.setDouble    stmt cnt value)
            Long               (.setLong      stmt cnt value)
            Short              (.setShort     stmt cnt value)
            Integer            (.setInt       stmt cnt value)
            java.net.URL       (.setUrl       stmt cnt value)
            java.sql.Date      (.setDate      stmt cnt value)
            java.util.Date     (let [value (java.sql.Date. (.getTime value))]
                                 (.setDate    stmt cnt value))
            java.sql.Time      (.setTime      stmt cnt value)
            java.sql.Timestamp (.setTimestamp stmt cnt value))
          (recur (rest env) (inc cnt)))))))

(defn load-driver
  "Load the named JDBC driver. Has to be called once before accessing
  the database."
  [driver]
  (Class/forName driver)
  nil)

;; SQL COMPILATION ==========================================

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

(defmulti compile-sql
  "Compile the given SQL statement for the given database."
  {:arglists '([stmt db])}
  (fn [stmt db] [(stmt :type) db]))

(defmethod compile-sql [::Select ::AnyDB]
  [stmt _]
  (let [{:keys [columns tables predicates col-aliases table-aliases]} stmt
        cols  (str-cat ","
                      (map (fn [spec]
                             (let [col (column-from-spec spec)]
                               (-> spec
                                 compile-function
                                 (compile-alias col col-aliases)
                                 ->string)))
                           columns))
        tabls (str-cat ","
                      (map (fn [spec]
                             (let [table (table-from-spec spec)]
                               (-> spec
                                 (compile-alias table table-aliases)
                                 ->string)))
                           tables))
        stmnt (list* "SELECT" cols
                     "FROM"   tabls
                     (when predicates
                       (list "WHERE" (infixed predicates))))]
    (str-cat " " stmnt)))

(defmethod compile-sql [::OrderedSelect ::AnyDB]
  [stmt db]
  (let [{:keys [query order columns]} stmt]
    (str-cat " " [(compile-sql query db)
                  "ORDER BY"
                  (str-cat "," (map ->string columns))
                  (condp = order
                    :ascending  "ASC"
                    :descending "DESC")])))

(defmethod compile-sql [::GroupedSelect ::AnyDB]
  [stmt db]
  (let [{:keys [query columns]} stmt]
    (str-cat " " [(compile-sql query db)
                  "GROUP BY"
                  (str-cat "," (map ->string columns))])))

(defmethod compile-sql [::HavingSelect ::AnyDB]
  [stmt db]
  (let [{:keys [query predicates]} stmt]
    (str-cat " " [(compile-sql query db)
                  "HAVING"
                  (compile-function predicates)])))

(defmethod compile-sql [::DistinctSelect ::AnyDB]
  [stmt db]
  (apply str "SELECT DISTINCT" (drop 6 (-> stmt :query (compile-sql db)))))

(defmethod compile-sql [::Union ::AnyDB]
  [stmt db]
  (let [{:keys [queries all]} stmt]
    (str "(" (str-cat " " (interpose (if all
                                       ") UNION ALL ("
                                       ") UNION (")
                                     (map #(compile-sql % db) queries)))
         ")")))

(defmethod compile-sql [::Intersect ::AnyDB]
  [stmt db]
  (let [{:keys [queries]} stmt]
    (str "(" (str-cat " " (interpose ") INTERSECT ("
                                     (map #(compile-sql % db) queries)))
         ")")))

(defmethod compile-sql [::Difference ::AnyDB]
  [stmt db]
  (let [{:keys [queries]} stmt]
    (str "(" (str-cat " " (interpose ") MINUS ("
                                     (map #(compile-sql % db) queries)))
         ")")))

(defmethod compile-sql [::Insert ::AnyDB]
  [stmt _]
  (let [{:keys [table columns]} stmt]
    (str-cat " " ["INSERT INTO" table "("
                  (str-cat "," (map ->string columns))
                  ") VALUES ("
                  (str-cat "," (take (count columns) (repeat "?")))
                  ")"])))

(defmethod compile-sql [::Update ::AnyDB]
  [stmt _]
  (let [{:keys [table columns predicates]} stmt]
    (str-cat " " ["UPDATE" table
                  "SET"    (str-cat "," (map (comp
                                               #(str % " = ?")
                                               ->string)
                                             columns))
                  "WHERE"  (infixed predicates)])))

(defmethod compile-sql [::Delete ::AnyDB]
  [stmt _]
  (let [{:keys [table predicates]} stmt]
    (str-cat " " ["DELETE FROM" table
                  "WHERE"       (infixed predicates)])))

(defmulti compile-sql-alter
  "Sub method to compile ALTER statements."
  {:arglists '([stmt db])}
  (fn [stmt db] [(stmt :subtype) db]))

(defmethod compile-sql [::AlterTable ::AnyDB]
  [stmt db]
  (compile-sql-alter stmt db))

(defmethod compile-sql-alter [::Add ::AnyDB]
  [stmt _]
  (let [{:keys [table action options keycoll]} stmt]
    (str-cat " " ["ALTER TABLE" table action
                  (str-cat " " options)
                  (if (= '(primary key) options)
                    (str "(" keycoll ")" )
                    (->comma-sep keycoll))])))

(defmethod compile-sql-alter [::Change ::AnyDB]
  [stmt _]
  (let [{:keys [table action options keycoll]} stmt]
    (str-cat " " ["ALTER TABLE" table action
                  (str-cat " " options) keycoll])))

(defmethod compile-sql-alter [::Modify ::AnyDB]
  [stmt _]
  (let [{:keys [table column new-type]} stmt]
    (str-cat " " ["ALTER TABLE" table "MODIFY" column new-type])))

(defmethod compile-sql-alter [::DropPrimary ::AnyDB]
  [stmt _]
  (let [{:keys [table]} stmt]
    (str "ALTER TABLE " table " DROP PRIMARY KEY")))

(defmethod compile-sql-alter [::Drop ::AnyDB]
  [stmt _]
  (let [{:keys [table target target-type]} stmt]
    (str-cat " " ["ALTER TABLE" table "DROP"
                  (case-str #(.toUpperCase (str %)) target-type)
                  (->comma-sep target)])))

(defmethod compile-sql [::CreateTable ::AnyDB]
  [stmt _]
  (let [{:keys [table columns]} stmt]
    (let [cols (str-cat "," (map (fn [[col type]]
                                   (str (->string col) " " type))
                                 (partition 2 columns)))]
      (str-cat " " ["CREATE TABLE"
                    table
                    "(" cols ")"]))))

(defmethod compile-sql [::DropTable ::AnyDB]
  [stmt _]
  (let [{:keys [table if-exists]} stmt]
    (str-cat " " ["DROP TABLE"
                  (when if-exists
                    "IF EXISTS")
                  table])))

;; SQL EXECUTION ============================================

(defn prepare-statement
  "Return a prepared statement for the given SQL statement in the
  context of the given connection."
  [sql-stmt conn]
  (doto (.prepareStatement conn (compile-sql sql-stmt ::AnyDB))
    (set-env (sql-stmt :env))))

(defn result-seq
  "This is basically a rip-off of Clojure's resultset-seq, which also
  closes the result set, when the last row was realized in the seq."
  [#^java.sql.ResultSet result-set]
    (let [meta-info  (.getMetaData result-set)
          idxs       (range 1 (inc (.getColumnCount meta-info)))
          keys       (map (comp keyword
                                #(.toLowerCase %)
                                #(.getColumnName meta-info %))
                          idxs)
          row-struct (apply create-struct keys)
          row-values (fn [] (map (fn [#^Integer i] (.getObject result-set i)) idxs))
          rows       (fn thisfn []
                       (if (.next result-set)
                         (lazy-cons (apply struct row-struct (row-values))
                                    (thisfn))
                         (do
                           (.close result-set)
                           nil)))]
      (rows)))

; Unfortunately, multifns don't support custom hierarchies.
(derive ::Select         ::ExecuteQuery)
(derive ::OrderedSelect  ::Select)
(derive ::GroupedSelect  ::Select)
(derive ::DistinctSelect ::Select)
(derive ::HavingSelect   ::Select)
(derive ::Union          ::ExecuteQuery)
(derive ::Intersect      ::ExecuteQuery)
(derive ::Difference     ::ExecuteQuery)
(derive ::Update         ::ExecuteUpdate)
(derive ::Insert         ::ExecuteUpdate)
(derive ::Delete         ::ExecuteUpdate)

(defn with-type
  "Bless the given statement with the given type."
  [stmt type]
  (assoc stmt :type type))

(defn in-transaction*
  "Execute thunk wrapped into a savepoint transaction."
  [conn thunk]
  (let [savepoint (.setSavepoint conn)]
     (try
       (let [result (thunk)]
         (.releaseSavepoint conn savepoint)
         result)
       (catch Exception e
         (.rollback conn savepoint)
         (throw e)))))

(defmacro in-transaction
  "Execute body wrapped into a savepoint transaction."
  [conn & body]
  `(in-transaction* ~conn (fn [] ~@body)))

(defmulti execute-sql
  "Execute the given SQL statement in the context of the given connection
  as obtained by with-connection."
  {:arglists '([sql-stmt conn])}
  (fn [sql-stmt conn] (sql-stmt :type))
  :default ::Execute)

(defmethod execute-sql ::Execute
  [sql-stmt conn]
  (let [prepd-stmt (prepare-statement sql-stmt conn)]
    (.execute prepd-stmt)
    prepd-stmt))

(defmethod execute-sql ::ExecuteQuery
  [sql-stmt conn]
  (-> sql-stmt
    (with-type ::Execute)
    (execute-sql conn)
    .getResultSet
    result-seq))

(defmethod execute-sql ::ExecuteUpdate
  [sql-stmt conn]
  (-> sql-stmt
    (with-type ::Execute)
    (execute-sql conn)
    .getUpdateCount))

(defmethod execute-sql ::LetQuery
  [sql-stmt conn]
  ((sql-stmt :fn) conn))

(defmethod execute-sql ::Batch
  [sql-stmt conn]
  (doall (map #(execute-sql % conn) (sql-stmt :statements))))

;; INTERFACE ================================================

(defmacro with-connection
  [[connection connection-info] & body]
  `(io! "Database interaction cannot happen in a transaction"
     (let [conn-info# ~connection-info]
       (with-open [~connection (java.sql.DriverManager/getConnection
                                 (:jdbc-url conn-info#)
                                 (:username conn-info#)
                                 (:password conn-info#))]
         ~@body))))

(defn run*
  " Driver for run - Dont call directly "
  [conn-info ast wfunc]
  (with-connection [open-conn conn-info]
    (wfunc (execute-sql ast open-conn))))

(defmacro run
  " Takes 3 arguments: A vector whos first element is connection-info and the second
                       is a placeholder for the results returned by the query

                       The second argument is an AST produced by Query.
                       Finally, a body for execution which has access to the results.

    Ex: (let [db1 (make-connection-info ...)]
          (run [db1 myresults]
           (query [col1 col2] database.table1 (> col1 col2))
           (doseq [result myresults]
             (do x y z to 'result')))
        - or -
        (run db1 (insert-into table1 name 'Frank' age 22)) "
  ([[connection-info results] ast & body]
     `(run* ~connection-info ~ast (fn [~results] ~@body)))
  ([connection-info ast]
     `(with-connection [open-connection# ~connection-info]
        (execute-sql ~ast open-connection#))))

;; UTILITIES ===============================================

(defn pa
  " pa=Print AST, helper func for debugging purposes "
  [ast]
  (doseq [entry ast]
    (prn entry)))


(defn vb
  " vb=View batch, helper func for debugging purposes "
  [ast]
  (doseq [row (:statements ast)]
    (println (:sql row))))

(defmacro print-rows
  [con query]
  `(run [~con results#]
        ~query
        (doseq [row# results#]
          (println row#))))






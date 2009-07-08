;; (c) 2008,2009 Lau B. Jensen <lau.jensen {at} bestinclass.dk
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

;; GLOBALS ==============================================

;; CONNECTION ==============================================

(defstruct connection-info
  :jdbc-url :username :password)

(defn make-connection-info
  " Given the arguments, this will return a hash-map which serves are your connection
    information for ex. with-connection "
  ([protocol host username password]
     (struct connection-info (format "jdbc:%s:%s" protocol host) username password))
  ([protocol host]
     (struct connection-info (format "jdbc:%s:%s" protocol host) nil nil)))

;; MACROS ==================================================

(defn set-env
  [#^PreparedStatement stmt env]
  (when (pos? (count env))
    (loop [env (seq env)
           cnt 1]
      (when env
        (let [value (first env)]
          (if (nil? value)
            ; FIXME: This does not work!
            (.setNull stmt cnt java.sql.Types/NULL)
            (condp instance? value
              String             (.setString    stmt cnt value)
              Float              (.setFloat     stmt cnt value)
              Double             (.setDouble    stmt cnt value)
              Long               (.setLong      stmt cnt value)
              Short              (.setShort     stmt cnt value)
              Integer            (.setInt       stmt cnt value)
              java.net.URL       (.setURL       stmt cnt value)
              java.sql.Date      (.setDate      stmt cnt value)
              java.util.Date     (let [value (java.sql.Date.
                                               (.getTime #^java.util.Date value))]
                                   (.setDate    stmt cnt value))
              java.sql.Time      (.setTime      stmt cnt value)
              java.sql.Timestamp (.setTimestamp stmt cnt value)))
          (recur (next env) (inc cnt)))))))

(defn load-driver
  "Load the named JDBC driver. Has to be called once before accessing
  the database."
  [driver]
  (try
   (clojure.lang.RT/classForName driver)
   (catch Exception e
     (throw
      (Exception. "The driver could not be loaded, please verify thats its found on the classpath")))))

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
  "Takes a quasiquoted form and infixes the operaters.

   (infixed `(+ 5 5)) => (5 + 5) "
  [form]
  (str "(" (str-cat " " (interpose (->string (first form))
                                   (map compile-function (rest form))))
       ")"))

(defn compile-function
  [col-spec]
  "Compile a function specification into a string.

   (compile-function (= 25 (sum count)) => (25 = sum(count)) "
  (if (or (seq? col-spec) (list? col-spec) (vector? col-spec))
    (let [[function col & args] col-spec]
      (if (= (sql-function-type function) :infix)
        (infixed col-spec)
        (str function "(" (str-cat "," (cons (->string col) args)) ")")))
    col-spec))

(def sql-hierarchy
  (atom (-> (make-hierarchy)
          (derive java.sql.Connection ::Generic)
          (derive ::Select            ::ExecuteQuery)
          (derive ::Join              ::Select)
          (derive ::InnerJoin         ::Join)
          (derive ::LeftJoin          ::Join)
          (derive ::RightJoin         ::Join)
          (derive ::FullJoin          ::Join)
          (derive ::OrderedSelect     ::Select)
          (derive ::GroupedSelect     ::Select)
          (derive ::DistinctSelect    ::Select)
          (derive ::HavingSelect      ::Select)
          (derive ::Union             ::ExecuteQuery)
          (derive ::Intersect         ::ExecuteQuery)
          (derive ::Difference        ::ExecuteQuery)
          (derive ::Update            ::ExecuteUpdate)
          (derive ::Insert            ::ExecuteUpdate)
          (derive ::Delete            ::ExecuteUpdate))))

(defmulti compile-sql
  "Compile the given SQL statement for the given database."
  {:arglists '([stmt db])}
  (fn [stmt db] [(stmt :type) (class db)])
  :hierarchy sql-hierarchy)

(defn compile-column-spec
  [columns aliases]
  (str-cat ","
           (map (fn [spec]
                  (let [col (column-from-spec spec)]
                    (-> spec
                      compile-function
                      (compile-alias col aliases)
                      ->string)))
                columns)))

(defn compile-table-spec
  [tables aliases]
  (str-cat ","
           (map (fn [spec]
                  (let [table (table-from-spec spec)]
                    (-> spec
                      (compile-alias table aliases)
                      ->string)))
                tables)))

(defmethod compile-sql [::Select ::Generic]
  [stmt _]
  (let [{:keys [columns tables predicates column-aliases table-aliases]} stmt
        cols  (compile-column-spec columns column-aliases)
        tabls (compile-table-spec tables table-aliases)
        stmnt (list* "SELECT" cols
                     "FROM"   tabls
                     (when predicates
                       (list "WHERE" (infixed predicates))))]
    (str-cat " " stmnt)))

(defmethod compile-sql [::Join ::Generic]
  [stmt _]
  (let [join-types {::InnerJoin "INNER"
                    ::LeftJoin  "LEFT"
                    ::RightJoin "RIGHT"
                    ::FullJoin  "FULL"}
        {:keys [query]} stmt
        {:keys [columns tables predicates column-aliases table-aliases]} query
        cols  (compile-column-spec columns column-aliases)
        left  (compile-table-spec [(first tables)] table-aliases)
        right (compile-table-spec [(second tables)] table-aliases)
        stmnt (list "SELECT" cols
                    "FROM"   left
                    (join-types (:type stmt))
                    "JOIN"   right
                    "ON"     (infixed predicates))]
    (str-cat " " stmnt)))

(defmethod compile-sql [::FullJoin ::EmulateFullJoin]
  [stmt db]
  (let [query (:query stmt)
        {:keys [columns tables predicates column-aliases table-aliases]} query
        cols  (compile-column-spec columns column-aliases)
        left  (compile-table-spec [(first tables)] table-aliases)
        right (compile-table-spec [(second tables)] table-aliases)
        stmnt (list "SELECT" cols
                    "FROM"   left
                    "LEFT JOIN" right
                    "ON"     (infixed predicates)
                    "UNION ALL"
                    "SELECT" cols
                    "FROM"   right
                    "LEFT JOIN" left
                    "ON"     (infixed predicates)
                    "WHERE"  (second predicates) "IS NULL")]
    (str-cat " " stmnt)))

(prefer-method compile-sql
               [::FullJoin ::EmulateFullJoin]
               [::Select ::Generic])

(prefer-method compile-sql
               [::FullJoin ::EmulateFullJoin]
               [::Join ::Generic])

(defmethod compile-sql [::OrderedSelect ::Generic]
  [stmt db]
  (let [{:keys [query order columns]} stmt]
    (str-cat " " [(compile-sql query db)
                  "ORDER BY"
                  (str-cat "," (map ->string columns))
                  (condp = order
                    :ascending  "ASC"
                    :descending "DESC")])))

(defmethod compile-sql [::GroupedSelect ::Generic]
  [stmt db]
  (let [{:keys [query columns]} stmt]
    (str-cat " " [(compile-sql query db)
                  "GROUP BY"
                  (str-cat "," (map ->string columns))])))

(defmethod compile-sql [::HavingSelect ::Generic]
  [stmt db]
  (let [{:keys [query predicates]} stmt]
    (str-cat " " [(compile-sql query db)
                  "HAVING"
                  (compile-function predicates)])))

(defmethod compile-sql [::DistinctSelect ::Generic]
  [stmt db]
  (apply str "SELECT DISTINCT" (drop 6 (-> stmt :query (compile-sql db)))))

(defmethod compile-sql [::Union ::Generic]
  [stmt db]
  (let [{:keys [queries all]} stmt]
    (str "(" (str-cat " " (interpose (if all
                                       ") UNION ALL ("
                                       ") UNION (")
                                     (map #(compile-sql % db) queries)))
         ")")))

(defmethod compile-sql [::Intersect ::Generic]
  [stmt db]
  (let [{:keys [queries]} stmt]
    (str "(" (str-cat " " (interpose ") INTERSECT ("
                                     (map #(compile-sql % db) queries)))
         ")")))

(defmethod compile-sql [::Difference ::Generic]
  [stmt db]
  (let [{:keys [queries]} stmt]
    (str "(" (str-cat " " (interpose ") MINUS ("
                                     (map #(compile-sql % db) queries)))
         ")")))

(defmethod compile-sql [::Insert ::Generic]
  [stmt _]
  (let [{:keys [table columns]} stmt]
    (str-cat " " ["INSERT INTO" table "("
                  (str-cat "," (map ->string columns))
                  ") VALUES ("
                  (str-cat "," (take (count columns) (repeat "?")))
                  ")"])))

(defmethod compile-sql [::Update ::Generic]
  [stmt _]
  (let [{:keys [table columns predicates]} stmt]
    (str-cat " " ["UPDATE" table
                  "SET"    (str-cat "," (map (comp
                                               #(str % " = ?")
                                               ->string)
                                             columns))
                  "WHERE"  (infixed predicates)])))

(defmethod compile-sql [::Delete ::Generic]
  [stmt _]
  (let [{:keys [table predicates]} stmt]
    (str-cat " " ["DELETE FROM" table
                  "WHERE"       (infixed predicates)])))

(defmulti compile-sql-alter
  "Sub method to compile ALTER statements."
  {:arglists '([stmt db])}
  (fn [stmt db] [(stmt :subtype) db]))

(defmethod compile-sql [::AlterTable ::Generic]
  [stmt db]
  (compile-sql-alter stmt db))

(defmethod compile-sql-alter [::Add ::Generic]
  [stmt _]
  (let [{:keys [table action options keycoll]} stmt]
    (str-cat " " ["ALTER TABLE" table action
                  (str-cat " " options)
                  (if (= '(primary key) options)
                    (str "(" keycoll ")" )
                    (->comma-sep keycoll))])))

(defmethod compile-sql-alter [::Change ::Generic]
  [stmt _]
  (let [{:keys [table action options keycoll]} stmt]
    (str-cat " " ["ALTER TABLE" table action
                  (str-cat " " options) keycoll])))

(defmethod compile-sql-alter [::Modify ::Generic]
  [stmt _]
  (let [{:keys [table column new-type]} stmt]
    (str-cat " " ["ALTER TABLE" table "MODIFY" column new-type])))

(defmethod compile-sql-alter [::DropPrimary ::Generic]
  [stmt _]
  (let [{:keys [table]} stmt]
    (str "ALTER TABLE " table " DROP PRIMARY KEY")))

(defmethod compile-sql-alter [::Drop ::Generic]
  [stmt _]
  (let [{:keys [table target target-type]} stmt]
    (str-cat " " ["ALTER TABLE" table "DROP"
                  (case-str #(.toUpperCase (str %)) target-type)
                  (->comma-sep target)])))

(defmethod compile-sql [::CreateTable ::Generic]
  [stmt _]
  (let [{:keys [table columns]} stmt]
    (str "CREATE TABLE " table " ("
         (str-cat "," (map #(str (first %) " " (second %)) columns))
         ")")))

(defmethod compile-sql [::DropTable ::Generic]
  [stmt _]
  (let [{:keys [table if-exists]} stmt]
    (str-cat " " ["DROP TABLE"
                  (when if-exists
                    "IF EXISTS")
                  table])))

(defmethod compile-sql [::Raw ::Generic]
  [stmt _]
  (:statement stmt))

;; SQL EXECUTION ============================================

(defn prepare-statement
  "Return a prepared statement for the given SQL statement in the
  context of the given connection."
  {:tag PreparedStatement}
  [sql-stmt #^Connection conn]
  (doto (.prepareStatement conn (compile-sql sql-stmt conn))
    (set-env (into [] (sql-stmt :env)))))

(defn in-transaction*
  "Execute thunk wrapped into a savepoint transaction."
  [#^Connection conn thunk]
  (let [auto-commit-state (.getAutoCommit conn)]
    (try
      (.setAutoCommit conn false)
      (let [savepoint (.setSavepoint conn)]
        (try
          (let [result (thunk)]
            (.releaseSavepoint conn savepoint)
            result)
          (catch Exception e
            (.rollback conn savepoint)
            (throw e))))
      (finally
        (.setAutoCommit conn auto-commit-state)))))

(defmacro in-transaction
  "Execute body wrapped into a savepoint transaction."
  [conn & body]
  `(in-transaction* ~conn (fn [] ~@body)))

(defmulti execute-sql
  "Execute the given SQL statement in the context of the given connection
  as obtained by with-connection."
  {:arglists '([sql-stmt conn])}
  (fn [sql-stmt conn] (sql-stmt :type))
  :default  ::Execute
  :hierarchy sql-hierarchy)

(defmethod execute-sql ::Execute
  [sql-stmt conn]
  (let [prepd-stmt (prepare-statement sql-stmt conn)]
    (.execute prepd-stmt)
    prepd-stmt))

(defmethod execute-sql ::ExecuteQuery
  [sql-stmt conn]
  (let [stmt       ((get-method execute-sql ::Execute) sql-stmt conn)
        result-set (.getResultSet #^PreparedStatement stmt)]
    (resultset-seq result-set)))

(defmethod execute-sql ::ExecuteUpdate
  [sql-stmt conn]
  (let [stmt ((get-method execute-sql ::Execute) sql-stmt conn)]
    (.getUpdateCount #^PreparedStatement stmt)))

(defmethod execute-sql ::LetQuery
  [sql-stmt conn]
  ((sql-stmt :fn) conn))

(defmethod execute-sql ::Batch
  [sql-stmt conn]
  (in-transaction conn
    (doall (map #(execute-sql % conn) (sql-stmt :statements)))))

(defmethod execute-sql ::Raw
  [sql-stmt conn]
  (let [stmt       ((get-method execute-sql ::Execute) sql-stmt conn)
        result-set (.getResultSet #^PreparedStatement stmt)]
    (resultset-seq stmt)))

;; INTERFACE ================================================

(defn with-connection*
  "Open the given database connection and calls thunk with the connection.
  Takes care that the connection is closed after thunk returns."
  [conn-info thunk]
  (io! "Database interaction cannot happen in a transaction"
       (with-open [conn (java.sql.DriverManager/getConnection
                          (:jdbc-url conn-info)
                          (:username conn-info)
                          (:password conn-info))]
         (thunk conn))))

(defmacro with-connection
  "Open the database described by the given connection-info and bind
  it to connection. Then execute body."
  [[connection connection-info] & body]
  `(with-connection* ~connection-info (fn [~connection] ~@body)))

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

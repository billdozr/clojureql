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

;; SQL EXECUTION ============================================

(defn prepare-statement
  "Return a prepared statement for the given SQL statement in the
  context of the given connection."
  [sql-stmt conn]
  (doto (.prepareStatement conn (sql-stmt :sql))
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

(defmulti
  #^{:arglists '([sql-stmt conn])
     :doc
  "Execute the given SQL statement in the context of the given connection
  as obtained by with-connection."}
  execute-sql
  (fn [sql-stmt conn] (sql-stmt :type))
  ::Execute)

(defmethod execute-sql ::ExecuteQuery
  [sql-stmt conn]
  (-> sql-stmt
    (prepare-statement conn)
    .executeQuery
    result-seq))

(defmethod execute-sql ::ExecuteUpdate
  [sql-stmt conn]
  (-> sql-stmt
    (prepare-statement conn)
    .executeUpdate))

(defmethod execute-sql ::Execute
  [sql-stmt conn]
  (-> sql-stmt
    (prepare-statement conn)
    .execute))

(defmethod execute-sql ::LetQuery
  [sql-stmt conn]
  ((sql-stmt :fn) conn))

;; INTERFACE ================================================

(defmacro with-connection
  [[connection connection-info] & body]
  `(with-open [~connection (java.sql.DriverManager/getConnection
                            (:jdbc-url ~connection-info)
                            (:username ~connection-info)
                            (:password ~connection-info))]
     ~@body))

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

(defmacro print-rows
  [con query]
  `(run [~con results#]
        ~query
        (doseq [row# results#]
          (println row#))))






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

(defn connect-info
  [protocol host username password]
  (struct connection-info (format "jdbc:%s://%s" protocol host) username password))

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
            java.util.Date     (.setDate      stmt cnt value)
            java.sql.Time      (.setTime      stmt cnt value)
            java.sql.Timestamp (.setTimestamp stmt cnt value))
          (recur (rest env) (inc cnt)))))))

(defn load-driver
  "Load the named JDBC driver. Has to be called once before accessing
  the database."
  [driver]
  (Class/forName driver)
  nil)

(defmacro with-connection
  [connection-info connection & body]
  `(with-open [~connection (java.sql.DriverManager/getConnection
                            (:jdbc-url ~connection-info)
                            (:username ~connection-info)
                            (:password ~connection-info))]
     ~@body))

(defmacro run
  " Takes 3 arguments: A vector whos first element is connection-info and the second
                       is a placeholder for the results returned by the query

                       The second argument is an AST produced by Query.
                       Finally, a body for execution which has access to the results.

    Ex: (let [db1 (connect-info ...)]
          (run [db1 myresults]
           (query [col1 col2] database.table1 (> col1 col2))
           (doseq [result myresults]
             (do x y z to 'result')))
        - or -  
        (run db1 (insert-into table1 name 'Frank' age 22)) "
  ([vec ast & body]
     (let [connection-info (first vec)
           results         (second vec)]        
       `(with-connection ~connection-info open-connection#
          (let [prepStmt# (.prepareStatement open-connection# (:sql ~ast))]
            (set-env prepStmt# (:env ~ast))
            (with-open [feed# (.executeQuery prepStmt#)]
              (let [~results (resultset-seq feed#)]
                ~@body))))))
  ([connection-info ast]
     `(with-connection ~connection-info open-connection#
        (let [prepStmt# (.prepareStatement open-connection# (:sql ~ast))]
          (set-env prepStmt# (:env ~ast))
            (.executeUpdate prepStmt#)))))

;; UTILITIES ===============================================

(defmacro print-rows
  [con query]
  `(run [~con results#]
        ~query
        (doseq [row# results#]
          (println row#))))
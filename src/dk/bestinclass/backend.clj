;; Copyright (c) 2008 Lau B. Jensen <lau.jensen {at} bestinclass.dk
;;                    Meikel Brandmeyer <mb {at} kotka.de>
;; All rights reserved.
;;
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file LICENSE.txt at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by the
;; terms of this license. You must not remove this notice, or any other, from
;; this software.

(ns dk.bestinclass.backend
  (:import
   (dk.bestinclass.clojureql)
   (java.sql DriverManager Driver SQLException)))

;; CONNECTION ==============================================

(defstruct connection-info
  :jdbc-url
  :username
  :password)

(defn connect-info
  [protocol host username password]
  (struct connection-info (format "jdbc:%s://%s" protocol host) username password))

;; MACROS ==================================================

(defn batch-add
  [stmt env]
  (when (pos? (count env))
    (loop [env env x 1]
      (when env
        (let [cls     (class (first env))
              val     (first env)]
          (cond (= Integer                cls)
                (doto stmt (.setInt       x val))
                (= String                 cls)
                (doto stmt (.setString    x (str val)))
                (= java.util.Date         cls)
                (doto stmt (.setDate      x val))
                (= Double                 cls)
                (doto stmt (.setDouble    x val))
                (= Float                  cls)
                (doto stmt (.setFloat     x val))
                (= Long                   cls)
                (doto stmt (.setLong      x val))
                (= Short                  cls)
                (doto stmt (.setShort     x val))
                (= java.sql.Time          cls)
                (doto stmt (.setTime      x val))
                (= java.sql.Timestamp     cls)
                (doto stmt (.setTimestamp x val))
                (= java.net.URL           cls)
                (doto stmt (.setURL       x val)))
          (recur (rest env) (inc x)))))
    (. stmt addBatch)))

(defmacro run
  " Takes 2 arguments: A vector whos first element is connection-info and the second
                       is a placeholder for the results returned by the query, the third
                       and final element is an AST produced by Query.
                       An AST produced by (query ...)
                       A body for execution which has access to the results.

    Ex: (let [db1 (connect-info ...) myresults []]
          (run myresults (query [col1 col2] database.table1 (> col1 col2))
             (doseq [result myresults]
              (do x y z to 'result')))) "              
  [vec & body]
  (let [connection (first vec)
        results    (second vec)
        ast        (last vec)]
    `(do
       (Class/forName "com.mysql.jdbc.Driver")
       (with-open [open-connection# (java.sql.DriverManager/getConnection (:jdbc-url ~connection)
                                                                          (:username ~connection)
                                                                          (:password ~connection))
                   prepStmt# (.prepareStatement open-connection#          (:sql ~ast))]
         (batch-add prepStmt# (:env ~ast))
         (with-open [feed# (.executeQuery prepStmt#)]
           (let [~results (resultset-seq feed#)]
             ~@body))))))





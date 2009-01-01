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

;; GLOBALS =================================================

(defstruct sql-connection
  :host
  :protocol
  :username
  :password
  :connection)

(def *connection* (ref (struct sql-connection
                               "127.0.0.1/mysql"       ; Host
                               "mysql"                 ; protocol
                               "testql"                ; Username
                               "test"                  ; Password
                               false)))


;; CONNECTION ==============================================

(defmacro with-connection
  [& body]
  `(do
     (Class/forName "com.mysql.jdbc.Driver")
     (let [jdbc-url# (format "jdbc:%s://%s"
                             ~(:protocol @*connection*)
                             ~(:host @*connection*))]
        (try
         (with-open [open-con (DriverManager/getConnection jdbc-url#
                                                      ~(:username @*connection*)
                                                      ~(:password @*connection*))]
           ~@body)
           (catch SQLException exceptionSql#
             (println exceptionSql#))))))


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
  " Takes 3 arguments: A container for the results returned by the query
                       An AST produced by (query ...)
                       A body for execution which has access to the results.

    Ex: (let [myresults []]
          (run myresults (query [col1 col2] database.table1 (> col1 col2))
             (doseq [result myresults]
              (do x y z to 'result')))) "              
  [results ast & body]
  `(do
     (Class/forName "com.mysql.jdbc.Driver")
     (let [jdbc-url#  (format "jdbc:%s://%s" ~(:protocol @*connection*)
                                             ~(:host     @*connection*))]
       (with-open [open-connection# (java.sql.DriverManager/getConnection jdbc-url#
                                                                 ~(:username @*connection*)
                                                                 ~(:password @*connection*))
                   prepStmt# (.prepareStatement open-connection#  (:sql ~ast))]
         (batch-add prepStmt# (:env ~ast))
         (with-open [feed# (.executeQuery prepStmt#)]
           (let [~results (resultset-seq feed#)]
             ~@body))))))





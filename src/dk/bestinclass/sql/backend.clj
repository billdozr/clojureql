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

(ns dk.bestinclass.sql.backend
  (:import (java.sql DriverManager Driver SQLException)))

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

(defmulti compile-ast (fn [ast] (:type ast)))

;; COMPILER ================================================

(defn comma-separate
  "Takes a sequence (list/vector) and seperates the elements by commas

   Ex. (comma-separate ['hi 'there]) => 'hi,there' "
 [coll]
 (apply str
        (interpose "," coll)))

(defn cherry-pick
  [lst & cherries]
  (if (symbol? lst)
    (.trim (str lst))
    (when (< (reduce max cherries) (count lst))
      (.trim
       (apply str
              (for [cherry cherries]
                (str (nth lst cherry) " ")))))))g
    
(defn- infixed
  " Contributed by Chouser.

    Ex: (infixed (and (> x 5) (< x 10))) =>
                  X > 5 AND X < 10 "
  [e]
  (let [f (fn f [e]
            (if-not (list? e)
              [(str e)]
              (let [[p & r] e]
                (if (= p `unquote)
                  r
                  (apply concat (interpose [(str " " p " ")] (map f r)))))))]
    (apply str (f e))))

(defmethod compile-ast ::Select
  [ast]
  (let [cols (str "(" (comma-separate (:columns ast)) ")")
        tabs (str "(" (comma-separate (:tables ast))  ")")]                  
    (.trim
     (str
      "SELECT " cols " "
      "FROM "   tabs " "
      (when-not (nil? (:predicates ast)))
        (str "WHERE " (infixed (:predicates ast)))))))



;; CONNECTION ==============================================

(defmacro with-connection
  [& body]
  `(do
     (Class/forName "com.mysql.jdbc.Driver")
     (let [jdbc-url# (format "jdbc:%s://%s"
                             ~(:protocol @*connection*)
                             ~(:host @*connection*))]
        (try
         (with-open [con# (DriverManager/getConnection jdbc-url#
                                                      ~(:username @*connection*)
                                                      ~(:password @*connection*))]
           ~@body)
           (catch SQLException exceptionSql#
             (println exceptionSql#))))))

(defn with-results
  [ast]
  (resultset-seq
   (.executeQuery
    (compile-ast
     ast))))

(defmacro execute-sql
  [& body]
  `(do
     (with-connection
         (with-results
           ~@body))))
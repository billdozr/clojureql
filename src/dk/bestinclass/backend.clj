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

(ns dk.bestinclass.clojureql
  (:import (java.sql DriverManager Driver SQLException)))

;; GLOBALS =================================================

(defstruct sql-connection
  :host
  :username
  :password
  :connection)

(def *connection* (ref (struct sql-connection
                               "127.0.0.1"
                               "testql"
                               "testpassword"
                               false)))


;; CONNECTION ==============================================

(defn make-connection
  [& args]
  (let [{host :host
         user :username
         pass :password} (apply hash-map args)]
    (Class/forName "org.apache.derby.jdbc.EmbeddedDriver"))
  nil)

(defn run-select
  [username password]
  (Class/forName "com.mysql.jdbc.Driver")
  (let [jdbc-url (str "jdbc:mysql://127.0.0.1:3306/mysql")]
    (try
     (with-open [con (DriverManager/getConnection jdbc-url
                                                 username
                                                 password)]
      ;(compile-ast (sql (query [owner title] list)))
       (println "Connected!"))
     (catch SQLException x
       (println x)))))
       
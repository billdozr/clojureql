;; TEST ====================================================


(ns dk.bestinclass.clojureql.demos.mysql
  (:gen-class)
  (:require
     [dk.bestinclass.clojureql :as sql]))

; Adapt the following connection-info to your local database.
(def *conn-info*
     (sql/make-connection-info "mysql"              ; Type
                               "//localhost/cql"    ; Adress and db (cql)
                               "cql"                ; Username
                               "cql"))              ; Password

(sql/load-driver "com.mysql.jdbc.Driver")

(load "common")
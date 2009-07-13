;; TEST ====================================================


(ns dk.bestinclass.clojureql.demos.postgres
  (:gen-class)
  (:require
     [dk.bestinclass.clojureql :as sql]
     dk.bestinclass.clojureql.backend.postgres))

; Adapt the following connection-info to your local database.
(def *conn-info*
  (sql/make-connection-info "postgresql"         ; Type
                            "//localhost/cql"    ; Adress and db (cql)
                            "cql"                ; Username
                            "cql"))              ; Password

(sql/load-driver "org.postgresql.Driver")

(load "common")

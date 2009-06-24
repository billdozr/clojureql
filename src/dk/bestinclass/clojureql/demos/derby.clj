(ns dk.bestinclass.clojureql.demos.derby
  (:gen-class)
  (:require
     [dk.bestinclass.clojureql :as sql]
     [dk.bestinclass.clojureql.backend.derby :as derby]))

; Load the Derby embedded driver.
(derby/load-embedded-driver)

(def *conn-info* (sql/make-connection-info "derby"
                                           "derby.demo;create=true"
                                           ""
                                           ""))

(load "common")

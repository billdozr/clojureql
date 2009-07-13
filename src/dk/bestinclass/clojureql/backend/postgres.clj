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

(clojure.core/ns dk.bestinclass.clojureql.backend.postgres
  (:require
     [dk.bestinclass.clojureql :as cql]
     [dk.bestinclass.clojureql.util :as util]))

(defmethod cql/compile-sql
  [::cql/CreateTable org.postgresql.PGConnection]
  [stmt db]
  (let [{:keys [table columns options]} stmt
        columns (map (fn [column]
                       (let [col-str (util/->string (first column))]
                         (str col-str
                              (when (= col-str (util/->string (:auto-inc options)))
                                " SERIAL")
                              (when (= col-str (util/->string (:auto-inc options)))
                                " NOT NULL"))))
                      columns)]
    (util/str-cat " " ["CREATE TABLE" table "(" (util/str-cat "," columns)
                       (when-let [p-key (:primary-key options)]
                         (util/str-cat " " ["PRIMARY KEY ("
                                            (util/->string p-key)
                                            ")"]))
                       ")"])))

(prefer-method cql/compile-sql
              [::cql/CreateTable org.postgresql.PGConnection]
              [::cql/CreateTable ::cql/Generic])

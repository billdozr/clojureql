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

(clojure.core/ns dk.bestinclass.clojureql.backend.derby
  (:require
     [dk.bestinclass.clojureql :as cql]))

(defmethod cql/compile-sql
  [::cql/CreateTable org.apache.derby.impl.jdbc.EmbedConnection]
  [stmt _]
  (let [{:keys [table columns options]}          stmt
        {:keys [primary-key non-nulls auto-inc]} options
        non-nulls   (set (cql/->vector non-nulls))
        auto-inc    (set (cql/->vector auto-inc))
        columns     (map (fn [[column col-type]]
                           (str column " " col-type
                                (when (contains? non-nulls column)
                                  " NOT NULL")))
                         columns)
        primary-key (when primary-key
                      (let [primary-key (cql/->vector primary-key)]
                        (str "PRIMARY KEY ("
                             (cql/str-cat "," primary-key)
                             ")")))]
    (str "CREATE TABLE " table " ("
         (cql/str-cat "," columns)
         (when primary-key (str "," primary-key))
         ")")))

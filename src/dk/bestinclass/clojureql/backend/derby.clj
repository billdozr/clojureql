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
  (:import
     clojure.lang.RT)
  (:require
     [dk.bestinclass.clojureql :as cql]
     [dk.bestinclass.clojureql.util :as util]))

(defn load-embedded-driver
  "Load the embedded derby driver."
  []
  (let [klass (RT/classForName "org.apache.derby.jdbc.EmbeddedDriver")]
    (.newInstance klass)
    nil))

(defn load-client-driver
  "Load the derby client driver."
  []
  (let [klass (RT/classForName "org.apache.derby.jdbc.ClientDriver")]
    (.newInstance klass)
    nil))

(defmethod cql/compile-sql
  [::cql/Join org.apache.derby.impl.jdbc.EmbedConnection]
  [stmt db]
  (if (= (:join stmt) :full)
    ; Special case: Derby does not support full JOIN. Emulate it.
    (let [query (:query stmt)
          {:keys [columns tables predicates column-aliases table-aliases]} query
          cols  (cql/compile-column-spec columns column-aliases)
          left  (cql/compile-table-spec [(first tables)] table-aliases)
          right (cql/compile-table-spec [(second tables)] table-aliases)
          stmnt (list "SELECT" cols
                      "FROM"   left
                      "LEFT JOIN" right
                      "ON"     (cql/infixed predicates)
                      "UNION ALL"
                      "SELECT" cols
                      "FROM"   right
                      "LEFT JOIN" left
                      "ON"     (cql/infixed predicates)
                      "WHERE"  (second predicates) "IS NULL")]
      (util/str-cat " " stmnt))
    ((get-method cql/compile-sql [::cql/Join ::cql/Generic]) stmt db)))


(defmethod cql/compile-sql
  [::cql/CreateTable org.apache.derby.impl.jdbc.EmbedConnection]
  [stmt _]
  (let [{:keys [table columns options]}          stmt
        {:keys [primary-key non-nulls auto-inc]} options
        non-nulls   (set (util/->vector non-nulls))
        auto-inc    (set (util/->vector auto-inc))
        columns     (map (fn [[column col-type]]
                           (str column " " col-type
                                (when (contains? non-nulls column)
                                  " NOT NULL")
                                (when (contains? auto-inc column)
                                  " GENERATED ALWAYS AS IDENTITY")))
                         columns)
        primary-key (when primary-key
                      (let [primary-key (util/->vector primary-key)]
                        (str "PRIMARY KEY ("
                             (util/str-cat "," primary-key)
                             ")")))]
    (str "CREATE TABLE " table " ("
         (util/str-cat "," columns)
         (when primary-key (str "," primary-key))
         ")")))

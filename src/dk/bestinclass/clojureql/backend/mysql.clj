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

(clojure.core/ns dk.bestinclass.clojureql.backend.mysql
  (:import
     clojure.lang.RT)
  (:require
     [dk.bestinclass.clojureql :as cql]
     [dk.bestinclass.clojureql.util :as util]))

(defmethod cql/compile-sql
  [::cql/CreateTable com.mysql.jdbc.Connection]
  [stmt db]
  (let [{:keys [table
                columns
                options]}   stmt]
    (apply str 
           (list "CREATE TABLE " table
                 " ("
                 (util/str-cat ", "
                               (map #(str (first %) " " (second %)
                                          (when (= (first %) (:not-null options))
                                            " NOT NULL ")
                                          (when (= (first %) (:auto-inc options))
                                            " AUTO_INCREMENT "))
                                    columns))
                 (when-not (nil? (:primary-key options))
                   (format ",PRIMARY KEY (`%s`)" (:primary-key options)))
                 ") "))))
    
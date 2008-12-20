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

(ns dk.bestinclass.clojureql)

;; GLOBALS ============================================

(defstruct sql-connection :host
                          :username
                          :password
                          :live)

(def *connection* (ref (struct sql-connection 0 0 0 false)))


;; CONNECTION =========================================

(defn make-connection
  [& args]
  (let [{host :host
         user :username
         pass :password} (apply hash-map args)]
    (Class/forName "org.apache.derby.jdbc.EmbeddedDriver"))
  nil)

;; TRANSACTIONS =======================================

(defn query [] nil)
(defn xalter [] nil)
(defn create [] nil)

(defstruct  sql-query
  :columns :tables :predicates :column-aliases :table-aliases)

(defmulti sql* (fn [_ form] (first form)))

(defmethod sql* 'query
  [env [_ col-spec table-spec pred-spec]]
  (let [check-alias
        (fn [[specs aliases] spec]
          (cond
           (vector? spec) (vector (conj specs (first spec))
                                  (conj aliases spec))
           (symbol? spec) (vector (conj specs spec) aliases)
           :else          (vector specs aliases)))
        col-spec    (if (vector? col-spec) col-spec (vector col-spec))
        [col-spec col-aliases]
        (reduce (fn [[specs aliases] spec]
                  (if (or (symbol? spec) (vector? spec))
                    (check-alias [specs aliases] spec)
                    (let [prefix (name (first spec))]
                      (reduce (fn [specs-aliases col]
                                (let [col (if (vector? col)
                                            (vector (symbol
                                                     (str prefix
                                                          "."
                                                          (name (first col))))
                                                    (second col))
                                            (symbol (str prefix "." col)))]
                                  (check-alias specs-aliases col)))
                              [specs aliases]
                              (rest spec)))))
                [nil {}]
                col-spec)
        tables-spec (if (vector? table-spec) table-spec (vector table-spec))
        [table-spec table-aliases]
        (reduce check-alias [nil {}] table-spec)]
    (struct sql-query col-spec table-spec nil col-aliases table-aliases)))

(defmacro sql
  [vars form]
  `(let [env# (into {} (list ~@(map #(vector (list 'quote %) %) vars)))]
     (sql* env# ~(list 'quote form))))

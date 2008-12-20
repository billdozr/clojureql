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
                          :password)

(def *connection* (ref (struct sql-connection 0 0 0)))


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

(defstruct
  sql-query
  :columns :tables :predicates :column-aliases :table-aliases)

(defn- ->vector
  [x]
  (cond
    (vector? x) x
    (symbol? x) (vector x)
    (string? x) (vector x)
    :else       (throw (Exception.
                         "only Symbols, Strings or Vectors are allowed"))))

(defn- check-alias
  [[specs aliases] [orig as aka]]
  (if (= as :as)
    (vector (conj specs orig)
            (conj aliases [orig aka]))
    (vector (conj specs orig)
            aliases)))

(defn- fix-prefix
  [[prefix & cols]]
  (let [spref (name prefix)
        reslv (fn [c] (symbol (str spref "." (name c))))]
    (map (fn [col]
           (if (vector? col)
             (vec (cons (reslv (col 0)) (rest col)))
             (reslv col)))
         cols)))

(defmulti sql* (fn [_ form] (first form)))

(defmethod sql* 'query
  ([env [query col-spec table-spec]]
   (sql* env (list query col-spec table-spec nil)))
  ([env [_ col-spec table-spec pred-spec]]
   (let [col-spec   (->vector col-spec)
         col-spec   (mapcat (fn [s] (if (seq? s) (fix-prefix s) (list s)))
                            col-spec)
         col-spec   (map ->vector col-spec)
         [col-spec col-aliases]
                    (reduce (fn [specs-aliases spec]
                              (check-alias specs-aliases spec))
                            [nil {}]
                            col-spec)
         table-spec (->vector table-spec)
         table-spec (map ->vector table-spec)
         [table-spec table-aliases]
                    (reduce check-alias [nil {}] table-spec)]
     (struct sql-query col-spec table-spec nil col-aliases table-aliases))))

(defmacro sql
  [vars form]
  `(let [env# (into {} (list ~@(map #(vector (list 'quote %) %) vars)))]
     (sql* env# ~(list 'quote form))))

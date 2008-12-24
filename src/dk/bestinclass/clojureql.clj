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


;; DEFINITIONS =============================================

(defmulti sql* (fn [_ form] (first form)))
(defmulti compile-ast (fn [ast] (:type ast)))

(defstruct sql-query
  :type :columns :tables :predicates :column-aliases :table-aliases)

(defstruct sql-insert :type :table :values)

;; HELPERS =================================================

(defn- ->vector
  "Takes 1 argument and converts it into a vector"
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
  " Takes a prefix and a series of columns and prepends the prefix
    to every column.

    Ex. (fix-prefix ['table1 'a 'b 'c]) => (table1.a table2.a table3.a) "
  [[prefix & cols]]
  (let [spref (name prefix)
        reslv (fn [c] (symbol (str spref "." (name c))))]
    (map (fn [col]
           (if (vector? col)
             (vec (cons (reslv (col 0)) (rest col)))
             (reslv col)))
         cols)))

;; SQL-BUILDING ============================================

(defmethod sql* 'query
  [env [_ col-spec table-spec pred-spec]]
  (let [col-spec   (->vector col-spec)
        col-spec   (mapcat (fn [s] (if (seq? s) (fix-prefix s) (list s)))
                           col-spec)
        col-spec   (map ->vector col-spec)
        [col-spec col-aliases]
                   (reduce check-alias [nil {}] col-spec)
        table-spec (->vector table-spec)
        table-spec (map ->vector table-spec)
        [table-spec table-aliases]
                   (reduce check-alias [nil {}] table-spec)]
    (struct sql-query ::Select col-spec table-spec pred-spec
            col-aliases table-aliases)))


(defmethod sql* 'insert-into
  [env [_ table & col-val-pairs]]
  (if (even? (count col-val-pairs))
    (struct sql-insert ::Insert table (apply hash-map col-val-pairs))
    (throw (Exception. "column/value pairs not balanced"))))

(defmacro sql
  " sql dispatches our Sql-Statement with all arguments fitted into
    a hashmap.

   Ex.  (sql (query [developer language id] employees (= language 'Clojure'))) "
  ([form]
   `(sql* (hash-map) ~(list 'quote form)))
  ([vars form]
   `(let [env# (into {} (list ~@(map #(vector (list 'quote %) %) vars)))]
      (sql* env# ~(list 'quote form)))))


;; COMPILER ================================================

(defn comma-seperate
  "Takes a sequence (list/vector) and seperates the elements by commas

   Ex. (comma-seperate ['hi 'there]) => 'hi,there' "
 [coll]
 (apply str
        (interpose "," coll)))
 

(defn cherry-pick
  [lst & cherries]
  (if (symbol? lst)
    (.trim (str lst))
    (when (< (reduce max cherries) (count lst))
      (.trim
       (apply str
              (for [cherry cherries]
                (str (nth lst cherry) " ")))))))
    
(defn infixed
  " Takes a Lisp-style mathematical expression and converts the
    prefixed operator to an infixed one

    Ex. (infixed (or (> x 5) (< x 10)) => x > 5 OR x < 10 "
  [statement]
  (if (or (= (str (nth statement 0)) "or") (= (str (nth statement 0)) "and"))
    (cherry-pick (map #(cherry-pick % 1 0 2) statement) 1 0 2)
    (cherry-pick statement 1 0 2)))

(defmethod compile-ast ::Select
  [ast]
  (let [cols (str "(" (comma-seperate (:columns ast)) ")")
        tabs (str "(" (comma-seperate (:tables ast))  ")")]                  
    (.trim
     (str
      "SELECT " cols " "
      "FROM "   tabs " "
      (when-not (nil? (:predicates ast)))
        (str "WHERE " (infixed (:predicates ast)))))))

(defn single-out
  [col cherries]
  (map #(nth col %) cherries))


(defn math-func?
  [expr]
  (some true? (map #(= (first expr) %)
                   '(> < >= <= = + -))))

(defn and-or?
  [expr]
  (or (= (str (first expr)) "or")
      (= (str (first expr)) "and")))

(defmacro where [e]
  (let [f (fn f [e]
            (if-not (list? e)
              [(str e)]
              (let [[p & r] e]
                (if (= p `unquote)
                  r
                  (apply concat (interpose [(str " " p " ")] (map f r)))))))]
    (list* `str (f e))))

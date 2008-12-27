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

(ns dk.bestinclass.clojureql
  (:require [dk.bestinclass.backend :as backend]))

;; DEFINITIONS =============================================

(defmulti sql* (fn [_ form] (first form)))
(defmulti compile-ast (fn [ast] (:type ast)))

(defstruct sql-query
  :type :columns :tables :predicates :column-aliases :table-aliases :env :sql)

(defstruct sql-insert
  :type :table :values :env :sql)

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

;; COMPILER ================================================

(defn pa
  " pa=Print AST, helper func for debugging purposes "
  [ast]
  (dorun
   (map println ast)))

(defn comma-separate
  "Takes a sequence (list/vector) and seperates the elements by commas

   Ex. (comma-separate ['hi 'there]) => 'hi,there' "
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
  " Contributed by Chouser.

    Ex: (infixed (and (> x 5) (< x 10))) =>
                  X > 5 AND X < 10 "
  [e]
  (let [f (fn f [e]
            (if-not (list? e)
              [(str e)]
              (let [[p & r] e]
                (if (= p `unquote)
                  "? "
                  ;(str "@" (first r))
                  (apply concat (interpose [(str " " p " ")] (map f r)))))))]
    (apply str (f e))))

(defn record
  [expr]
  (loop [expr expr env_record {}]
    (if-not (list? expr)
            env_record
            (let [[t v & r] expr]
              (if (list? v)
                (recur r (assoc env_record (keyword (str (last v))) (eval (last v))))
                (recur r env_record))))))

;; AST-BUILDING ============================================

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
                   (reduce check-alias [nil {}] table-spec)
        build-env  (fn build-env [preds]
                     (if (list? preds)
                       (let [[p & r] preds]
                         (if (= `unquote p)
                           (let [nam     (str (first r))
                                 val     (eval (first r))]
                             [(hash-map (keyword nam) val)])
                         (map #(last (build-env %)) r)))))]
    (struct sql-query ::Select col-spec table-spec pred-spec
            col-aliases table-aliases (apply merge (build-env pred-spec))
            (let [cols (str (comma-separate col-spec))
                  tabs (str (comma-separate table-spec))]               
              (.trim
               (str
                "SELECT " cols " "
                "FROM "   tabs " "
                (when-not (nil? pred-spec)
                  (str "WHERE " (infixed pred-spec)))))))))

(defmethod sql* 'insert-into
  [env [_ table & col-val-pairs]]
  (let [val-map  (apply hash-map col-val-pairs)]
    (if (even? (count col-val-pairs))
      (struct sql-insert ::Insert table val-map (record col-val-pairs)
              (let [val-names (apply str (interpose ", " (map #(str (name %))       (keys val-map))))
                    values    (apply str (interpose ", " (map #(if (list? %) "?" %) (vals val-map))))]
                (str "INSERT INTO " table " (" val-names ") VALUES (" values ")")))
      (throw (Exception. "column/value pairs not balanced")))))

(defmacro sql
  " sql dispatches our Sql-Statement with all arguments fitted into
    a hashmap/AST.

   Ex.  (sql (query [developer language id] employees (= language 'Clojure'))) "
  ([form]
   `(sql* (hash-map) ~(list 'quote form)))
  ([vars form]
   `(let [env# (into {} (list ~@(map #(vector (list 'quote %) %) vars)))]
      (sql* env# ~(list 'quote form)))))


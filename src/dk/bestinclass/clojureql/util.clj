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

(ns dk.bestinclass.clojureql.util
  "Utility functions used by the different parts of ClojureQL.")

(defn ->vector
  "Takes 1 argument and converts it into a vector"
  [thing]
  (cond
    (vector? thing)  thing
    (list? thing)    (vector thing)
    (string? thing)  (vector thing)
    (symbol? thing)  (vector thing)
    (keyword? thing) (vector thing)
    :else (throw (Exception.
                   "only Symbols, Keywords, Strings or Vectors are allowed"))))

(defn ->string
  "Converts an object into a string"
  [obj]
  (cond
    (string? obj)  obj
    (symbol? obj)  (name obj)
    (keyword? obj) (name obj)
    :else          (str obj)))

(defn column-from-spec
  "Try to get the column from a specificaton. Examples:
     (column-from-spec col) => col
     (column-from-spec (??? col ...) => col
     (column-from-spec [col :as ...]) => col
     (column-from-spec [(??? col ...) :as ...]) => col"
  [col-spec]
  (cond
    (vector? col-spec) (column-from-spec (first col-spec))
    (list? col-spec)   (second col-spec)
    :else              col-spec))

(defn table-from-spec
  "Try to get the table from a specificaton. Examples:
     (table-from-spec table) => table
     (table-from-spec [table :as ???]) => table"
  [table-spec]
  (cond
    (vector? table-spec) (first table-spec)
    :else                table-spec))

(defn check-alias
  " Takes an output style and a colspec, returns aliases

    (check-alias [[] {}] `[foo :as bar]) => [foo] {foo bar}"
  [[specs aliases] [orig as aka]]
  (if (= as :as)
    (vector (conj specs orig) (conj aliases [(column-from-spec orig) aka]))
    (vector (conj specs orig) aliases)))

(defn fix-prefix
  "Takes a prefix and a series of columns and prepends the prefix to
  every column.

  Example:
    (fix-prefix '[table1 :cols a b c]) => (table1.a table1.b table1.c)"
  [col-spec]
  (if (vector? col-spec)
    (let [[prefix prefix-for & cols] col-spec]
      (if (= prefix-for :cols)
        (let [str-prefix   (->string prefix)
              resolve-spec (fn resolve-spec [col]
                             (if (list? col)
                               (let [[function col & rst] col]
                                 (list* function (resolve-spec col) rst))
                               (symbol (str str-prefix "." (->string col)))))]
          (map (fn [col]
                 (if (vector? col)
                   (vec (cons (resolve-spec (first col)) (rest col)))
                   (resolve-spec col)))
               cols))
        [col-spec]))
    [col-spec]))

(defn self-eval?
  "Check whether the given form is self-evaluating."
  [obj]
  (or (keyword?  obj)
      (number?   obj)
      (instance? Character obj)
      (string?   obj)
      (nil?      obj)))

(defn flatten-map
  "Flatten the keys and values of a map into a list."
  [the-map]
  (reduce (fn [result entry] (-> result
                               (conj (key entry))
                               (conj (val entry))))
          [] the-map))

(defn unquote?
  "Tests whether the given form is of the form (unquote ...)."
  [form]
  (and (seq? form) (= (first form) `unquote)))

(defn quasiquote*
  "Worker for quasiquote macro. See docstring there. For use in macros."
  [form]
  (cond
    (self-eval? form) form
    (unquote? form)   (second form)
    (symbol? form)    (list 'quote form)
    (vector? form)    (vec (map quasiquote* form))
    (map? form)       (apply hash-map (map quasiquote* (flatten-map form)))
    (set? form)       (apply hash-set (map quasiquote* form))
    (seq? form)       (list* `list (map quasiquote* form))
    :else             (list 'quote form)))

(defmacro quasiquote
  "Quote the supplied form as quote does, but evaluate unquoted parts.

  Example: (let [x 5] (quasiquote (+ ~x 6))) => (+ 5 6)"
  [form]
  (quasiquote* form))

bv(defn str-cat
  "Concatenate collection to a string. The members are separated by separator."
  [sep coll]
  (apply str (interpose sep coll)))

(defn case-str
  " Takes 2 arguments, a method and a target. The method will be applied to the target
    or members of the target if its a vector.

    (case-str #(.toUpperCase %) ['clojureql']) => 'CLOJUREQL' "
  [fn-format target]
  (let [target (into [] target)
        seq-way (fn [_]
                  (fn-format (apply str (interpose " " _))))]
    (cond
     (string? target)    (fn-format target)
     (vector? target)    (seq-way target))))

(defn ->comma-sep
  " Will take a vector seperate the items with commas. If argument is not a
    vector, target is returned.

    (->comma-sep '' ['foo 'bar 'baz]) => 'foo, bar, baz' "
  ([target] (->comma-sep "" target))
  ([wrapper target]
     (if-not (vector? target)
             target
             (str (first wrapper) (str-cat ", "  target) (second wrapper)))))
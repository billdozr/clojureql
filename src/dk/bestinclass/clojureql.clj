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
    (class/forName "org.apache.derby.jdbc.EmbeddedDriver")
    (...)))

;; TRANSACTIONS =======================================

(defn query ...
(defn alter
(defn create ...

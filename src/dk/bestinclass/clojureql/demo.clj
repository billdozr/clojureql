;; TEST ====================================================


(ns dk.bestinclass.clojureql.demo
  (:gen-class)
  (:require
     [dk.bestinclass.clojureql :as sql]))

; First define in your database the following table.
;
; MySQL:
;
; CREATE TABLE `employees` (
;  `id` int(11) NOT NULL auto_increment,
;  `name` varchar(100) NOT NULL,
;  `language` varchar(100) NOT NULL,
;  `efficiency` double NOT NULL,
;  `iq` int(11) NOT NULL,
;  PRIMARY KEY  (`id`)
;  ) ENGINE=MyISAM AUTO_INCREMENT=27 DEFAULT CHARSET=latin1
;
; Sorry. create-table is not finished, yet.

; Adapt the following connection-info to your local database.
(def *conn-info* (sql/connect-info "mysql"
                                   "localhost/mysql"
                                   "mysqluser"
                                   "mysqlpsw"))


; Set the correct driver here.
(def *driver* "com.mysql.jdbc.Driver")

; Then simply run this namespace to execute some example queries.

(defn insert-data
  []
  (let [columns [    :name     :language   :efficiency   :iq ]
        data    [[   "Frank"   "Python"    0.75           82 ]
                 [   "Brian"   "OCaml"     0.9           119 ]
                 [   "John"    "Fortran"   0.1           122 ]
                 [   "Mark"    "PHP"       0.71          104 ]
                 [   "Peter"   "SBCL"      0.9           123 ]
                 [   "Jack D." "Haskell"   0.9           118 ]
                 [   "Mike"    "C++"       0.002         107 ]
                 [   "Vader"   "Pure Evil" 0.99          158 ]
                 [   "Arnold"  "Cobol"     0.24           24 ]
                 [   "Chouser" "Clojure"   1             192]]
        make-stmt (fn [nom language efficiency iq]
                    (sql/insert-into roster.employees
                                     name       ~nom
                                     language   ~language
                                     efficiency ~efficiency
                                     iq          ~iq))]
    (doseq [stmt (map #(apply make-stmt %) data)]
      (sql/run *conn-info* stmt))))

(defn -main
  [& args]
  (sql/load-driver *driver*)
  (insert-data)                          ; First populate the DB
  ; Query (and other statements) are first class objects. They can
  ; be stored in locals or passed to functions...
  (let [q (sql/query [id name language efficiency]  ; Columns to fetch
                     roster.employees               ; Schema/Db
                     (> efficiency 0.5))]           ; Taking only efficient developers
    (sql/run [*conn-info* results]       ; Then execute some SQL and work with the results
             (sql/order-by q             ; Order our query ...
                           :descending   ; ... descending by ...
                           efficiency)   ; ... the columns 'efficiency'.
      (doseq [row results]               ; The rest is up to you
  ; Another example of a high-level query:
  ; Dynamically create a new query, which transparently
  ; combines two subquery.
  (let [union-query (sql/let-query [q1 (sql/order-by
                                         (sql/query [id name language efficiency]
                                                    employees
                                                    (> efficiency 0.9))
                                         :descending
                                         efficiency)
                                    q2 (sql/order-by
                                         (sql/query [id name language efficiency]
                                                    employees
                                                    (< efficiency 0.5))
                                         :descending
                                         efficiency)]
                      (concat q1 q2))]
    (sql/print-rows *conn-info* union-query)))
        (println row))))

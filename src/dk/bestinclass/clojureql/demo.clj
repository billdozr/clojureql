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
;
; SQLite:
;
; CREATE TABLE employees (id PRIMARY KEY, name, language, effeciency)
;
; Sorry. create-table is not finished, yet.

; Adapt the following connection-info to your local database.
(def *conn-info* (sql/connect-info "mysql"
                                   "localhost/mysql"
                                   "mysqluser"
                                   "mysqlpsw"))


; Set the correct driver here.
;(def *driver* "org.sqlite.JDBC")
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
;  (insert-data)
  (sql/run [*conn-info* results]
           (sql/query [id name language efficiency] roster.employees (> efficiency 0.5))
           (doseq [row results]
             (println row))))


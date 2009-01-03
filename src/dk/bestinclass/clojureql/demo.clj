;; TEST ====================================================

(ns dk.bestinclass.clojureql.demo
  (:gen-class)
  (:require
     [dk.bestinclass.clojureql :as sql]))

; First define in your database the following table.
;
; MySQL:
;
; CREATE TABLE employees (
;   id int NOT NULL AUTO_INCREMENT,
;   name varchar(100)  NOT NULL,
;   language varchar(100)  NOT NULL,
;   effeciency DOUBLE  NOT NULL,
;   PRIMARY KEY (`id`));
;
; SQLite:
;
; CREATE TABLE employees (id PRIMARY KEY, name, language, effeciency)
;
; Sorry. create-table is not finished, yet.

; Adapt the following connection-info to your local database.
(def *conn-info*
  {:jdbc-url "jdbc:sqlite:ClojureQL_Demo.db" :username "" :password ""})

; Set the correct driver here.
(def *driver* "org.sqlite.JDBC")

; Then simply run this namespace to execute some example queries.

(defn insert-data
  []
  (let [columns [ :id :name     :language   :efficiency ]
        data    [[1   "Frank"   "Python"    0.75       ]
                 [2   "Brian"   "OCaml"     0.9        ]
                 [3   "John"    "Fortran"   0.1        ]
                 [4   "Mark"    "PHP"       0.71       ]
                 [5   "Peter"   "SBCL"      0.9        ]
                 [6   "Jack D." "Haskell"   0.9        ]
                 [7   "Mike"    "C++"       0.002      ]
                 [8   "Vader"   "Pure Evil" 0.99       ]
                 [9   "Arnold"  "Cobol"     0.24       ]
                 [10  "Chouser" "Clojure"   1          ]]
        make-stmt (fn [id nom language efficiency]
                    (sql/insert-into employees
                                     id         ~id
                                     name       ~nom
                                     language   ~language
                                     efficiency ~efficiency))]
    (doseq [stmt (map #(apply make-stmt %) data)]
      (sql/run *conn-info* stmt))))

(defn -main
  [& args]
  (sql/load-driver *driver*)
  (insert-data))

(comment "
dk.bestinclass.clojureql> (execute
                           (sql
                            (query [id name] developers.employees)))
{:name Frank, :id 1}

{:name Brian, :id 2}
{:name John, :id 3}
{:name Mark, :id 4}
{:name Peter, :id 5}
{:name Jack D., :id 6}
{:name Mike, :id 7}
{:name Vader, :id 8}
{:name Arnold, :id 9}
{:name Chouser, :id 10}
dk.bestinclass.clojureql> (execute
                           (sql
                            (query [id name] developers.employees
                                   (and (> id 5)
                                        (< id 8)))))
{:name Jack D., :id 6}
{:name Mike, :id 7}
")

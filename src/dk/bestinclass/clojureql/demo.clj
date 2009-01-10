;; TEST ====================================================


(ns dk.bestinclass.clojureql.demo
  (:gen-class)
  (:require
     [dk.bestinclass.clojureql :as sql]))

; Choose your database. Currently MySQL and SQLite are supported
; in the examples.
(def *database-system* :SQLite)

; Adapt the following connection-info to your local database.
(def *conn-info*
  (condp = *database-system*
    :MySQL  (sql/connect-info "mysql"
                              "localhost/mysql"
                              "mysqluser"
                              "mysqlpsw")
    :SQLite {:jdbc-url "jdbc:sqlite:demo.db"
             :username ""
             :password ""}))

(def *driver*
  (condp = *database-system*
    :MySQL  "com.mysql.jdbc.Driver"
    :SQLite "org.sqlite.JDBC"))

(defn run-and-show
  [query]
  (sql/run [*conn-info* rows] query
    (doseq [row rows]
      (prn row))))

(defn -main
  [& args]
  ; First load the driver. This is has to be done once
  ; on application startup.
  (sql/load-driver *driver*)

  ; Create the table we will use in the demo.
  (sql/run *conn-info* (condp = *database-system*
                         :MySQL  (sql/create-table StoreInformation
                                                   [StoreName "varchar(100)"]
                                                   [Sales "int(11)"]
                                                   [Date date])
                         :SQLite (sql/create-table StoreInformation
                                                   StoreName
                                                   Sales
                                                   Date)))

  ; Populate the table with data.
  (let [make-stmt (fn [[store sale date]]
                    (sql/insert-into StoreInformation
                                     StoreName ~store
                                     Sales     ~sale
                                     Date      ~date))
        data      [["Los Angeles"   1500 (java.util.Date. 1999 1 5)]
                   ["San Diego"      250 (java.util.Date. 1999 1 7)]
                   ["San Francisco"  300 (java.util.Date. 1999 1 8)]
                   ["Los Angeles"    100 (java.util.Date. 1999 1 8)]
                   ["Boston"         700 (java.util.Date. 1999 1 9)]]]
    (doseq [record data]
      (sql/run *conn-info* (make-stmt record))))

  ; Let's do a sample query.
  (println "SELECT StoreName FROM StoreInformation")
  (run-and-show (sql/query StoreName StoreInformation))

  ; Now let's modify the previous query to only return distinct
  ; results.
  (println "SELECT DISTINCT StoreName FROM StoreInformation")
  (run-and-show (sql/distinct!
                  (sql/query StoreName StoreInformation)))

  ; Where are the biggest sales?
  (println "SELECT StoreName FROM StoreInformation WHERE Sales > 1000")
  (run-and-show (sql/query StoreName
                           StoreInformation
                           (> Sales 1000)))

  ; Biggest sales and middlefield?
  (println "SELECT StoreName FROM StoreInformation WHERE Sales > 1000 OR (Sales < 500 AND Sales > 275)")
  (run-and-show (sql/query StoreName
                           StoreInformation
                           (or (> Sales 1000)
                               (and (< Sales 500)
                                    (> Sales 275)))))

  ; Let's check all stores containing an 'an'.
  (println "SELECT * FROM StoreInformation WHERE StoreName LIKE '%an%'")
  (run-and-show (sql/query *
                           StoreInformation
                           (like StoreName "%an%")))

  ; Now we check the hit parade of our stores.
  (println "SELECT * FROM StoreInformation ORDER BY Sales DESC")
  (run-and-show (sql/order-by (sql/query * StoreInformation)
                              :descending
                              Sales))

  ; What is the average of our sales?
  (println "SELECT avg(Sales) FROM StoreInformation")
  (run-and-show (sql/query (avg Sales) StoreInformation))

  ; What are the Sales for the different stores?
  (println "SELECT StoreName,sum(Sales) FROM StoreInformation GROUP BY StoreName")
  (run-and-show (sql/group-by (sql/query [StoreName (sum Sales)]
                                         StoreInformation)
                              StoreName))

  ; What are the top stores?
  (println "SELECT StoreName,sum(Sales) FROM StoreInformation GROUP BY StoreName HAVING sum(Sales) > 1200")
  (run-and-show (sql/having (sql/group-by (sql/query [StoreName (sum Sales)]
                                                     StoreInformation)
                                          StoreName)
                            (> (sum Sales) 1200)))

  ; Using aliases.
  (println "SELECT si.StoreName AS Store, sum(si.Sales) AS TotalSales FROM StoreInformation AS si GROUP BY si.StoreName")
  (run-and-show (sql/group-by (sql/query [[si :cols
                                           [StoreName :as Store]
                                           [(sum Sales) :as TotalSales]]]
                                         [[StoreInformation :as si]])
                              si.StoreName))

  ; Be truly DB agnostic...
  (println "On MySQL:      SELECT CONCAT(ColA, ColB) FROM table")
  (println "On Oracle:     SELECT ColA || ColB FROM table")
  (println "On SQL Server: SELECT ColA + ColB FROM table")
  (println "In ClojureQL:  One syntax to rule them all!")
  (run-and-show (sql/let-query [result (sql/group-by
                                         (sql/query [StoreName
                                                     [(sum Sales) :as TotalSales]]
                                                    StoreInformation)
                                         StoreName)]
                  (map #(hash-map :our-result (str (:storename %)
                                                   " sold $"
                                                   (:totalsales %) "!"))
                       result)))

  ; Cover our tracks.
  (sql/run *conn-info* (sql/drop-table StoreInformation)))

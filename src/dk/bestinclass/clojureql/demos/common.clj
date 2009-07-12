;; TEST ====================================================

(defn run-and-show
  [query]
  (sql/run [*conn-info* rows] query
    (doseq [row rows]
      (prn row))))

; Needed for homegrown predicate.
(def *low-perform-threshold* 300)

(swap! sql/where-clause-type assoc "is-low-performer?" ::LowPerformer)
(swap! sql/sql-function-type assoc "is-low-performer?" ::LowPerformer)

(defmethod sql/build-env ::LowPerformer
  [form env]
  [form (conj env *low-perform-threshold*)])

(defmethod sql/compile-function ::LowPerformer
  [form]
  (sql/compile-function (list '< (second form) "?")))

(defn -main
  [& args]
  ; Create the table we will use in the demo.
  (sql/run *conn-info* (sql/create-table StoreInformation
                                         [id int
                                          StoreName "varchar(100)"
                                          Sales int
                                          Date date]
                                         :primary-key  id
                                         :not-null     id
                                         :auto-inc     id))

  (sql/run *conn-info* (sql/create-table TownInformation
                                         [TownName "varchar(100)"
                                          Inhabitants int]))

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
                   ["Boston"         700 (java.util.Date. 1999 1 9)]
                   ["Berlin"         nil (java.util.Date. 1999 1 9)]]]
    (doseq [record data]
      (sql/run *conn-info* (make-stmt record))))

  (sql/with-connection [conn *conn-info*]
    (let [make-stmt (fn [[town inhabitants]]
                      (sql/insert-into TownInformation
                                       TownName    ~town
                                       Inhabitants ~inhabitants))
          data      [["Los Angeles"   2500000]
                     ["San Francisco" 1000000]
                     ["Boston"         500000]
                     ["Frankfurt"      900000]]]
      (doseq [record data]
        (sql/execute-sql (make-stmt record) conn))))

  ; Let's do a sample query.
  (println "SELECT StoreName FROM StoreInformation")
  (run-and-show (sql/query StoreName StoreInformation))

  ; Now let's modify the previous query to only return distinct
  ; results.
  (println "SELECT DISTINCT StoreName FROM StoreInformation")
  (run-and-show (sql/distinct!
                  (sql/query StoreName StoreInformation)))

  ; Where have we recorded more than 1000 sales?
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

  ; Oops - looks like we need to update 'Boztons' storename
  (println "UPDATE StoreInformation SET 'storename' = 'Boston' WHERE 'storename' = 'Bozton'")
  (sql/run *conn-info* (sql/update StoreInformation
                            [storename "Boston"]
                            (= storename "Bozton")))
  
  
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

  (println "SELECT StoreInformation.StoreName, TownInformation.Inhabitants FROM StoreInformation INNER JOIN TownInformation ON StoreInformation.StoreName = TownInformation.TownName")
  (run-and-show
    (sql/join :inner
              (sql/query [StoreInformation.StoreName TownInformation.Inhabitants]
                         [StoreInformation TownInformation]
                         (= StoreInformation.StoreName TownInformation.TownName))))

  (println "SELECT StoreInformation.StoreName, TownInformation.Inhabitants FROM StoreInformation LEFT JOIN TownInformation ON StoreInformation.StoreName = TownInformation.TownName")
  (run-and-show
    (sql/join :left
              (sql/query [StoreInformation.StoreName TownInformation.Inhabitants]
                         [StoreInformation TownInformation]
                         (= StoreInformation.StoreName TownInformation.TownName))))

  (println "SELECT StoreInformation.StoreName, TownInformation.Inhabitants FROM StoreInformation RIGHT JOIN TownInformation ON StoreInformation.StoreName = TownInformation.TownName")
  (run-and-show
    (sql/join :right
              (sql/query [StoreInformation.StoreName TownInformation.Inhabitants]
                         [StoreInformation TownInformation]
                         (= StoreInformation.StoreName TownInformation.TownName))))

  (println "SELECT StoreInformation.StoreName, TownInformation.Inhabitants FROM StoreInformation FULL JOIN TownInformation ON StoreInformation.StoreName = TownInformation.TownName")
  (run-and-show
    (sql/join :full
              (sql/query [StoreInformation.StoreName TownInformation.Inhabitants]
                         [StoreInformation TownInformation]
                         (= StoreInformation.StoreName TownInformation.TownName))))

  (println "SELECT StoreName FROM StoreInformation WHERE sales IS NULL")
  (run-and-show
    (sql/query StoreName StoreInformation (nil? Sales)))

  (println "SELECT StoreName FROM StoreInformation WHERE sales IS NOT NULL")
  (run-and-show
    (sql/query StoreName StoreInformation (not-nil? Sales)))

  (println "Again, this time with (not (nil? ..))")
  (println "SELECT StoreName FROM StoreInformation WHERE sales IS NOT NULL")
  (run-and-show
    (sql/query StoreName StoreInformation (not (nil? Sales))))

  (println "Domain predicate: is-low-performer?")
  (run-and-show
    (sql/query StoreName StoreInformation (is-low-performer? Sales)))

  ; Cover our tracks.
  (sql/run *conn-info* (sql/drop-table StoreInformation))
  (sql/run *conn-info* (sql/drop-table TownInformation))
  nil)

(comment
  ; Modify existing tables
  (println "ALTER TABLE StoreInformation ADD PRIMARY KEY ( id )")
  (sql/run *conn-info* (sql/alter-table StoreInformation add primary key id))

  (println "ALTER TABLE StoreInformation CHANGE Sales YearlySales int(11)")
  (sql/run *conn-info* (sql/alter-table StoreInformation change Sales YearlySales "int(11)"))

  (println "ALTER TABLE StoreInformation MODIFY Sales int(5)")
  (sql/run *conn-info* (sql/alter-table StoreInformation modify Sales "int(5)")))

;; TEST ====================================================

(ns dk.bestinclass.clojureql
  (:require [dk.bestinclass.backend :as sql]))

(defn run-all
  []
  ())

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


(comment "
  To replicate our test-table, run this on your MySQL server:

  1) Make a database called developers. Add a user and give appropriate rights.
     'USE developers;', and run the following:

  CREATE TABLE `roster`.`employees` (
  `id` int  NOT NULL AUTO_INCREMENT,
  `name` varchar(100)  NOT NULL,
  `language` varchar(100)  NOT NULL,
  `effeciency` DOUBLE  NOT NULL,
  `iq` int  NOT NULL,
  PRIMARY KEY (`id`)
  )
  ENGINE = MyISAM
  COMMENT = 'Table containing employee details for all current developers';

INSERT INTO employees
   (`id`, `name`, `language`, `effeciency`, `iq`)
VALUES
   (1, 'Frank', 'Python', 0.75, 85);

INSERT INTO employees
   (`id`, `name`, `language`, `effeciency`, `iq`)
VALUES
   (2, 'Brian', 'OCaml', 0.8, 110);

INSERT INTO employees
   (`id`, `name`, `language`, `effeciency`, `iq`)
VALUES
   (3, 'John', 'Fortran', 0.1, 120);

INSERT INTO employees
   (`id`, `name`, `language`, `effeciency`, `iq`)
VALUES
   (4, 'Mark', 'PHP', 0.71, 100);

INSERT INTO employees
   (`id`, `name`, `language`, `effeciency`, `iq`)
VALUES
   (5, 'Peter', 'SBCL', 0.9, 125);

INSERT INTO employees
   (`id`, `name`, `language`, `effeciency`, `iq`)
VALUES
   (6, 'Jack D.', 'Haskell', 0.2, 122);

INSERT INTO employees
   (`id`, `name`, `language`, `effeciency`, `iq`)
VALUES
   (7, 'Mike', 'C++', 0.002, 111);

INSERT INTO employees
   (`id`, `name`, `language`, `effeciency`, `iq`)
VALUES
   (8, 'Vader', 'Pure Evil', 0.99, 204);

INSERT INTO employees
   (`id`, `name`, `language`, `effeciency`, `iq`)
VALUES
   (9, 'Arnold', 'Cobol', 0.24, 100);

INSERT INTO employees
   (`id`, `name`, `language`, `effeciency`, `iq`)
VALUES
   (10, 'Chouser', 'Clojure', 1, 205);


")



 *   Clojure
 *   Copyright (c) Lau B. Jensen and Meikel Brandmeyer. All rights reserved.
 *   The use and distribution terms for this software are covered by the
 *   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
 *   which can be found in the file LICENSE.txt at the root of this distribution.
 *   By using this software in any fashion, you are agreeing to be bound by
 *    the terms of this license.
 *   You must not remove this notice, or any other, from this software.

ClojureQL is a joint venture between Lau B. Jensen and Meikel Brandmeyer,
started in 15. December 2008.

---------------------------------------------

This library is based on Clojure (http://www.clojure.org), which is a functional
Lisp developed by Rich Hickey to solve a number of problems relating to, among
other things, concurrency.

Initially we wanted to wrap SQL-statements into higher-order-functions, which
would enable the user to work with data in and out of database servers, without
abandoning his Lisp-syntax to write SQL statements. In our first tests, we quickly
recognized the need to restrict Clojure to total db-agnosticism as Meikel worked
on SqlLite and I on MySql. These two points are the primary considerations in all
our decision making: Is it true to Lisp, and does it work on all major SQL
implementations?

Consider the following:
---------------------------------------------
You need to create a database table, with a primary key.

On Mysql, this is the way to do it:
CREATE TABLE Customer (SID integer, Last_Name varchar(30), First_Name varchar(30), PRIMARY KEY (SID));

On Oracle, its:
CREATE TABLE Customer (SID integer PRIMARY KEY, Last_Name varchar(30), First_Name varchar(30));

In ClojureQL, its:
(create-table Customer [SID "int" Last_Name "varchar(30)" First_Name "varchar(30)"] :primary SID)

This works for all SQL implementations, and you dont even have to specificy which DB your working on!
(notice: types are given in literal type, to maximize compatibility)
---------------------------------------------

You can directly import our classes by placing them on your class-path and starting
Clojure:
         java -cp clojure.jar clojure.lang.Repl

This gives you a working REPL. From that REPL you can import our code using
         (use :reload 'dk.bestinclass.clojureql) RET

Where RET is pressing the Return key to execute. Once this returns nil, the classes
are loaded and you can begin working with your SQL DB, without paying any attention
to which type of Database youre working on, and its specific syntax.



We hope that you enjoy using ClojureQL and that you'll let us know if you come across bugs or if
you have feature requests.



/Lau B. Jensen & Meikel Brandmeyer/


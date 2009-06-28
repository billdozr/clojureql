# ClojureQL – making SQL dynamic

## Introduction

This library is based on ideas from [SchemeQL][]. It provides more or less
first class queries which can be further refined or modified via combinator
functions. Many of the database specifics can be abstracted away.

At the moment we provide special backends for Derby and MySQL. Further
code for other database systems is always appreciated...

The syntax is different to SQL, but abstracting the syntax it is (relatively)
easy to provide special operations for the different databases. With Strings
only, this would be much harder.

## Documentation

Please find detailed documentation in the [wiki][] on github. These are just
some examples to give you a feeling for ClojureQL.

Select all columns from a table.

        (query * table)

Select some specific columns from the table.

        (query [a b c] table)

Group by some column.

        (group-by (query [a (sum b)] table) a)

Note how group-by is a function of the query.

The union of two queries.

        (let [query1 (query [a b c] table)
              query2 (query [a b c] other-table)]
          (union query1 query2))

Queries defined in such a way are used with the run or with-connection
macros.

        (run *connection-info* some-query)
        (with-connection [connection *connection-info*]
          (execute-sql some-query connection))

## Installation

There are two ways to use ClojureQL.

### The Manual Way

Create a local.properties file in the root directory of the project. Specify
the clojure.jar property in this file to point to the location of your clojure.jar.
Run „ant“ to compile the Clojure files and create the jar file.

Add the jar file to your CLASSPATH and it will be available for use:

        (ns my.name.space
          (:require [dk.bestinclass.clojureql :as cql]
                    [dk.bestinclass.clojureql.backend.derby :as cql-derby]))

### The Ivy Way

Add the Kotka repository to your ivysettings.xml.

        <ivysettings>
                <!-- Load the standard Ivy settings. -->
                <include url="${ivy.default.settings.dir}/ivysettings.xml"/>
                <include url="http://kotka.de/ivy/ivysettings.xml"/>
        </ivysettings>

Now you can add a dependency to your [Ivy-enable project][Ivy] to automatically
fetch and use ClojureQL.

        <dependencies>
                <dependency org="dk.bestinclass" name="clojureql" rev="1.0.0"
                        conf="*->compiled,derby"/>
        </dependencies>

Note: the Ivy repository will be installed with the 1.0 release. For now
only the manual way is available.

## Copyright

Copyright © Lau B. Jensen and Meikel Brandmeyer. All rights reserved.
The use and distribution terms for this software are covered by the
Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
which can be found in the file LICENSE.txt at the root of this distribution.
By using this software in any fashion, you are agreeing to be bound by
the terms of this license.
You must not remove this notice, or any other, from this software.

ClojureQL is a joint venture between Lau B. Jensen and Meikel Brandmeyer,
started on 15. December 2008.

[SchemeQL]: http://schematics.sourceforge.net/schemeunit-schemeql.ps
[wiki]:     http://wiki.github.com/Lau-of-DK/clojureql
[Ivy]:      http://ant.apache.org/ivy/

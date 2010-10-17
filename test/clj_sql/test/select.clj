
(ns clj-sql.test.select
  "Builds SQL statements"
  (:use clojure.contrib.strint)
  (:require [clojure.contrib.string :as string])
  (:require [clj-sql.core :as sql]))


(defn- quote-name
  "Converts a keyword into a quoted name. Handles table.field for use
   in select statements."
  [n]
  (string/join "." (map sql/quote-name (string/split #"\." (name n)))))

(defn- as-clean-str
  "Converts field and table names to strings"
  [x]
  (cond
    (= :* x)       "*"
    (keyword? x)   (quote-name x)
    (string? x)    (sql/quote-name x)))

(defn- build-field-names
  "Builds the list of field names in an sql select statement"
  [fields]
  (cond
    (vector? fields)   (string/join ", " (map as-clean-str fields))
    (map? fields)      (string/join ", "
                                    (map (fn [[a b]]
                                           (str (as-clean-str a)
                                                " as "
                                                (as-clean-str b)))
                                         fields))))

(defn- build-table-names
  "Builds the list of table names in a select statement"
  [tables]
  (string/join ", " (map as-clean-str tables)))

(defn- build-where-clause
  "Builds a where clause from a map"
  [where-map]
  (let [pairs (map (fn [[a b]]
                     (str (as-clean-str a) "=" (as-clean-str b)))
                   where-map)]
    (if (empty? pairs)
      ""
      (str "where " (string/join ", " pairs)))))

(defn select
  "Runs an sql select and returns the results"
  ([fields from-tables]
     (select fields from-tables nil))
  ([fields from-tables where]
     (let [field-names (build-field-names fields)
           table-names (build-table-names from-tables)
           where-clause (build-where-clause where)
           s (<< "select ~{field-names} from ~{table-names} ~{where-clause}")]
       (sql/with-query-results res [s]
         (doall res)))))
 


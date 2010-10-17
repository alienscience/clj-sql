(ns #^{:doc "This is pretty much the same as clojure.contrib.sql,
except that it uses quoting for column and table names, which means
you can - if your database permits it - use more funky charcters in
your names. The most obvious character you'll want to use from Clojure
is '-', as in :user-id, etc"}
  clj-sql.core
  (:require [clojure.contrib [sql :as sql]]
            [clojure.contrib [def :only defalias]]
            [clojure.contrib.sql [internal :as internal]]
            [clojure.contrib [string :as str]])
  (:use (clojure.contrib [java-utils :only [as-str]]))
  (:import [java.sql Statement]))

(def #^{:doc "the character used for quoting table and column names. This is a String"}
     *quote-character* "\"")

(def #^{:doc "The regular expression that matches names that needn't be quoted"}
     *plain-name-re* #"^[a-zA-Z][a-zA-Z0-9_]*$")

(def #^{:doc "The regular expression that matches valid names"}
     *valid-name-re* #"^[a-zA-Z_<>\-+=\[\]\.\,\/\?][0-9a-zA-Z_<>\-+=\[\]\.\,\/\?]*$")

(defn quote-name [n]
  "Quote a table or column name.
Accepts strings and keywords. Names must match *valid-name-re*"
  (let [n (as-str n)]
    (if (re-matches *valid-name-re* n)
      (if (re-matches *plain-name-re* n)
        n
        (str *quote-character* n *quote-character*))
      (throw (Exception. (format "'%s' is not a valid name" n))))))

(defmacro
  #^{:doc "alias (using defalias) a bunch of vars from another namespace into the current one"
     :private true}
  alias-from [n & vars]
  `(do ~@(map #(list 'clojure.contrib.def/defalias % (symbol (name n) (name %))) vars)))

(alias-from clojure.contrib.sql
            find-connection connection with-connection connection
            transaction set-rollback-only is-rollback-only
            do-commands do-prepared with-query-results
            transaction set-rollback-only)

(defn- column-entry
  "Converts an entry in a column definition into a string"
  [x]
  (cond
    (keyword? x)    (quote-name x)
    (vector? x)     (str "(" (str/join "," (map quote-name x)) ")")
    (string? x)     x))

(defn- column-definition
  "Converts a vector containing a column definition into a string"
  [column-def]
  (str/join " " (map column-entry column-def)))

(defn- create-table-sql
  "Returns the sql that creates a table with the given specs"
  [name specs]
  (format
   "CREATE TABLE %s (%s)"
   (quote-name name)
   (str/join "," (map column-definition specs))))


(defn create-table
  "Creates a table on the open database connection given a table name and
  specs. Each spec is either a column spec: a vector containing a column
  name and optionally a type and other constraints, or a table-level
  constraint: a vector containing words that express the constraint. All
  words used to describe the table may be supplied as strings or keywords.
  Column names are quoted if needed, but only when they're specified as a keyword."
  [name & specs]
  (do-commands
   (create-table-sql name specs)))

(defn drop-table
  "Drops a table on the open database connection given its name, a string
  or keyword"
  [name]
  (do-commands
   (format "DROP TABLE %s" (quote-name name))))

(defn insert-values
  "Inserts rows into a table with values for specified columns only.
  column-names is a vector of strings or keywords identifying columns. Each
  value-group is a vector containing a values for each column in
  order. When inserting complete rows (all columns), consider using
  insert-rows instead."
  [table column-names & value-groups]
  (let [column-strs (map quote-name column-names)
        n (count (first value-groups))
        template (apply str (interpose "," (replicate n "?")))
        columns (if (seq column-names)
                  (format "(%s)" (apply str (interpose "," column-strs)))
                  "")]
    (apply do-prepared
           (format "INSERT INTO %s %s VALUES (%s)"
                   (quote-name table) columns template)
           value-groups)))

(defn insert-rows
  "Inserts complete rows into a table. Each row is a vector of values for
  each of the table's columns in order."
  [table & rows]
  (apply insert-values table nil rows))

(defn insert-records
  "Inserts records into a table. records are maps from strings or
  keywords (identifying columns) to values."
  [table & records]
  (doseq [record records]
    (insert-values table (keys record) (vals record))))

(defn delete-rows
  "Deletes rows from a table. where-params is a vector containing a string
  providing the (optionally parameterized) selection criteria followed by
  values for any parameters."
  [table where-params]
  (let [[where & params] where-params]
    (do-prepared
     (format "DELETE FROM %s WHERE %s"
             (quote-name table) where)
     params)))

(defn update-values
  "Updates values on selected rows in a table. where-params is a vector
  containing a string providing the (optionally parameterized) selection
  criteria followed by values for any parameters. record is a map from
  strings or keywords (identifying columns) to updated values."
  [table where-params record]
  (let [[where & params] where-params
        column-strs (map quote-name (keys record))
        columns (apply str (concat (interpose "=?, " column-strs) "=?"))]
    (do-prepared
     (format "UPDATE %s SET %s WHERE %s"
             (quote-name table) columns where)
     (concat (vals record) params))))

(defn update-or-insert-values
  "Updates values on selected rows in a table, or inserts a new row when no
  existing row matches the selection criteria. where-params is a vector
  containing a string providing the (optionally parameterized) selection
  criteria followed by values for any parameters. record is a map from
  strings or keywords (identifying columns) to updated values."
  [table where-params record]
  (transaction
   (let [result (update-values table where-params record)]
     (if (zero? (first result))
       (insert-values table (keys record) (vals record))
       result))))

;; the following is insert-with-id functionality

;;==== Internal functions ======================================================

(defn- join 
  "Joins the items in the given collection into a single string separated
   with the string separator."
  [separator col]
  (apply str (interpose separator col)))

(defn- sql-for-insert 
  "Converts a table identifier (keyword or string) and a hash identifying
   a record into an sql insert statement compatible with prepareStatement
    Returns [sql values-to-insert]"
  [table record]
  (let [table-name (quote-name table)
        columns (map quote-name (keys record))
        values (vals record)
        n (count columns)
        template (join "," (replicate n "?"))
        column-names (join "," columns)
        sql (format "insert into %s (%s) values (%s)"
                    table-name column-names template)]
    [sql values]))


;;==== Functions/macros for use by macros ======================================

(defn run-chained 
  "Runs the given database insert functions on the given
   database spec within a transaction. Each function is passed a hash
   identifying the keys of the previous inserts."
  [insert-fns]
  (transaction
   (loop [id {}
          todo insert-fns]
     (if (empty? todo)
       id
       (let [[table insert-fn] (first todo)
             inserted-id (insert-fn id)]
         (recur (assoc id table inserted-id)
                (rest todo)))))))

(defmacro build-insert-fns 
  "Converts a vector of [:table { record }] into a vector of database
   insert functions."
  [table-records]
  (vec
   (for [[table record] (partition 2 table-records)]
     `[~table (fn [~'id]
                (insert-record ~table ~record))])))


;;==== Functions/macros for external use =======================================


(defn do-insert 
  "Executes arbitary sql containing a single insert 
   and returns the autogenerated id of an inserted record if available.

     (do-insert \"insert into employees (name) values (?)\" [\"fred\"])"
  [sql param-group]
  (with-open [statement (.prepareStatement (connection)
                                           sql
                                           Statement/RETURN_GENERATED_KEYS)]
    (doseq [[index value] (map vector (iterate inc 1) param-group)]
      (.setObject statement index value))
    (if (< 0 (.executeUpdate statement))
      (if-let [rs (.getGeneratedKeys statement)]
        (with-open [rs rs]
          (-> rs resultset-seq first vals first))))))

(defn insert-record 
  "Equivalent of clojure.contrib.sql/insert-records that only inserts a single
   record but returns the autogenerated id of that record if available."
  [table record]
  (let [[sql param-group] (sql-for-insert table record)]
    (do-insert sql param-group)))
                          
(defmacro insert-with-id 
  "Insert records within a single transaction into the current datasource. 
   The record format is :table  { record-hash }. 
   The record hashes can optionally access a hashmap 'id' which holds the
   autogenerated ids of previous inserts keyed by the table name. e.g.
    
      (insert-with-id 
          :department {:name \"xfiles\"
                       :location \"secret\"}
          :employee   {:department (id :department)
                       :name \"Mr X\"})"
  [& table-records]
  `(let [insert-fns# (build-insert-fns ~table-records)]
     (run-chained insert-fns#)))


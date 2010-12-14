(ns clj-sql.core
  (:require [clojure.contrib [sql :as sql]]
            [clojure.contrib [def :only defalias]]
            [clojure.contrib.sql [internal :as internal]]
            [clojure.contrib [string :as string]]
            [clj-sql.resultset :as resultset])
  (:use (clojure.contrib [java-utils :only [as-str]]))
  (:import [java.sql Statement]))



(def #^{:doc "The regular expression that matches valid names"}
     valid-name-re #"^[a-zA-Z_<>\-+=\[\]\.\,\/\?][0-9a-zA-Z_<>\-+=\[\]\.\,\/\?]*$")

(def ^:dynamic #^{:doc "The current connection dbspec"}
     *current-dbspec* nil)

(def #^{:doc "Optional Writer that will be used for logging SQL"}
     *show-sql* nil)

(def #^{:doc "Hash/atom caching database metadata and keyed by connection db-spec"}
     db-meta-data (atom {}))

(defmacro
  #^{:doc "alias (using defalias) a bunch of vars from another namespace into the current one"
     :private true}
  alias-from [n & vars]
  `(do ~@(map #(list 'clojure.contrib.def/defalias % (symbol (name n) (name %))) vars)))

(alias-from clojure.contrib.sql
            find-connection connection
            transaction set-rollback-only is-rollback-only
            transaction set-rollback-only)

(defn log
  "Optionally logs the given strings to the Writer bound by *show-sql*"
  [& s]
  (if *show-sql*
    (doseq [l s]
      (.write *show-sql* (str l "\n")))))

(defn do-commands
  "Executes SQL commands on the open database connection."
  [& commands]
  (apply log commands)
  (with-open [stmt (.createStatement (connection))]
    (doseq [cmd commands]
      (.addBatch stmt cmd))
    (transaction
      (seq (.executeBatch stmt)))))

(defn do-prepared
  "Executes an (optionally parameterized) SQL prepared statement on the
open database connection. Each param-group is a seq of values for all of
the parameters."
  [sql & param-groups]
  (log sql)
  (with-open [stmt (.prepareStatement (connection) sql)]
    (doseq [param-group param-groups]
      (doseq [[index value] (map vector (iterate inc 1) param-group)]
        (.setObject stmt index value))
      (.addBatch stmt))
    (transaction
     (seq (.executeBatch stmt)))))

(defn- lc-keyword
  "A function that lowercases and keywords a string"
  [k]
  (keyword (.toLowerCase ^String k)))

(defn with-query-results*
  "Executes a query, then evaluates func passing in a seq of the results as
an argument. The first argument is a vector containing the (optionally
parameterized) sql query string followed by values for any parameters.
   This version of with-query-results also takes and optional keyword
argument, :key-fn, that will be used to map the column names of the result
set into keys of the returned maps. The default version of this mapping
function behaves in the same way as resultset-seq."
  [[sql & params :as sql-params] func
   &{:keys [keyfn] :or {keyfn lc-keyword}}]
  (log sql)
  (with-open [stmt (.prepareStatement (connection) sql)]
    (doseq [[index value] (map vector (iterate inc 1) params)]
      (.setObject stmt index value))
    (with-open [rset (.executeQuery stmt)]
      (func (resultset/as-seq rset keyfn)))))

(defmacro with-query-results
  "Executes a query, then evaluates body with results bound to a seq of the
results. sql-params is a vector containing a string providing
the (optionally parameterized) SQL query followed by values for any
parameters."
  [results sql-params & body]
    `(with-query-results* ~sql-params (fn [~results] ~@body)))

(defmacro with-query-results-keys
  "Executes a query, then evaluates body with results bound to a seq of the
results whose keys have been mapped with key-fn.
  sql-params is a vector containing a string providing
the (optionally parameterized) SQL query followed by values for any
parameters."
  [results keyfn sql-params & body]
    `(with-query-results* ~sql-params (fn [~results] ~@body) :keyfn ~keyfn))

(defn with-connection*
  "Evaluates func in the context of a new connection to a database
   then closes the connection."
  [dbspec func]
  (binding [*current-dbspec* dbspec]
    (internal/with-connection* dbspec func)))

(defmacro with-connection
  [db-spec & body]
  `(with-connection* ~db-spec (fn [] ~@body)))

(defn- set-db-meta!
  "Fills a cache with required meta data from the current connection"
  []
  (let [conn (connection)
        db-meta (.getMetaData conn)
        quote-character (.getIdentifierQuoteString db-meta)
        extra-name-chars (.getExtraNameCharacters db-meta)
        plain-name-re (re-pattern (str "^[a-zA-Z][a-zA-Z0-9_"
                                       extra-name-chars
                                       "]*$"))]
    (swap! db-meta-data
           assoc
           *current-dbspec*
           {:quote-character quote-character
            :plain-name-re plain-name-re})))

(defn- get-db-meta
  "Gets the database metadata for the current connection"
  []
  (if (nil? (@db-meta-data *current-dbspec*))
    (set-db-meta!))
  (@db-meta-data *current-dbspec*))

(defn quote-name 
  "Quote a table or column name.
   Accepts strings and keywords. Names must match valid-name-re"
  [n]
  (let [n (as-str n)
        db-meta (get-db-meta)]
    (if (re-matches (db-meta :plain-name-re) n)
      n
      (if (re-matches valid-name-re n)
        (let [quote-character (db-meta :quote-character)]
          (str quote-character n quote-character))
        (throw (Exception. (format "'%s' is not a valid name" n)))))))


(defn- column-entry
  "Converts an entry in a column definition into a string"
  [x]
  (cond
    (keyword? x)    (quote-name x)
    (vector? x)     (str "(" (string/join "," (map quote-name x)) ")")
    (string? x)     x))

(defn- column-definition
  "Converts a vector containing a column definition into a string"
  [column-def]
  (string/join " " (map column-entry column-def)))

(defn- create-table-sql
  "Returns the sql that creates a table with the given specs"
  [name specs]
  (format
   "CREATE TABLE %s (%s)"
   (quote-name name)
   (string/join "," (map column-definition specs))))


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

;;==== Internal functions for inserting ========================================

(defn- sql-for-insert 
  "Converts a table identifier (keyword or string) and a hash identifying
   a record into an sql insert statement compatible with prepareStatement
    Returns [sql values-to-insert]"
  [table record]
  (let [table-name (quote-name table)
        columns (map quote-name (keys record))
        values (vals record)
        n (count columns)
        template (string/join "," (replicate n "?"))
        column-names (string/join "," columns)
        sql (format "insert into %s (%s) values (%s)"
                    table-name column-names template)]
    [sql values]))


;;==== Insert functions/macros for use by macros ===============================

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


;;==== Insert functions/macros for external use ================================


(defn do-insert 
  "Executes arbitary sql containing a single insert 
   and returns the autogenerated id of an inserted record if available.

     (do-insert \"insert into employees (name) values (?)\" [\"fred\"])"
  [sql param-group]
  (log sql)
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


;;==== Query results cursor ====================================================

(defn with-query-results-cursor
  "Executes a query, then calls func (fn [res] ...) each time fetch-size has
   been retrieved from the database. The res argument to func is a seq of
   the results.
   sql-params is a vector containing a string providing
   the (optionally parameterized) SQL query followed by values for any
   parameters.
   Takes an optional keyword :keyfn that will be used to map column names
   into keys of the returned maps.
   This functions relies on the database and the JDBC driver supporting
   the .setFetchSize method on statement objects and is known not to
   use cursors with H2, Derby and Mysql.
     e.g
       (with-connection db
         (with-query-results-cursor
            50
            [\"select * from table where department = ?\" \"XFiles\"]
            (fn [res]
              ;; do something with a sequence of up to 50 maps
              )))"
  [fetch-size [sql & params :as sql-params] func
   &{:keys [keyfn] :or {keyfn lc-keyword}}]
  (log sql)
  (sql/transaction
   (with-open [stmt (.prepareStatement (connection) sql)]
     (.setFetchSize stmt fetch-size)
     (doseq [[index value] (map vector (iterate inc 1) params)]
       (.setObject stmt index value))
     (with-open [rset (.executeQuery stmt)]
       (func (resultset/as-seq rset keyfn))))))


;;==== DB Meta data functions ==================================================

(defn schemas
  "Returns a list of the schema names in the database."
  []
  (let [schemas (.getSchemas (.getMetaData (connection)))]
    (loop [has-next (.next schemas)
           res []]
      (if has-next
        (let [schema  (.getString schemas 1)]
          (recur (.next schemas)
                 (conj res schema)))
        res))))


(defn schema-objects
  "Returns a list of maps describing the objects in the database.  The
 maps include: :catalog, :schema, :name, :type and :remarks as per
 the JDBC spec."
  [& [schema]]
  (let [db-meta (.getMetaData (connection))
        ;; NB: "public" is the default for postgres
        tables  (.getTables db-meta nil (or schema "public")
                            "%" nil)]
    (loop [has-next (.next tables)
           res []]
      (if has-next
        (let [table {:catalog  (.getString tables  1)
                     :schema   (.getString tables  2)
                     :name     (.getString tables  3)
                     :type     (.getString tables  4)
                     :remarks  (.getString tables  5)
                     }]
          (recur (.next tables)
                 (conj res table)))
        res))))


(defn schema-tables [& [schema]]
  (filter #(= (:type %1)
              "TABLE")
          (schema-objects schema)))

(defn- range-sql [end]
  (range 1 (+ 1 end)))

(defn describe-table
  "Returns a list of column descriptions (maps) for the table.  The maps
  contain:
  :name, :catalog, :display-size, :type, :precision, :scale
  :is-auto-increment, :is-case-sensitive, :is-currency
  :is-definitely-writable, :is-nullable, :is-read-only
  :is-searchable, :is-signed, :is-writable."
  [table-name]
  (let [ps (.prepareStatement
            (connection)
            (format "SELECT * FROM %s WHERE 0 = 1" (quote-name table-name)))
        rs (.executeQuery ps)
        rs-meta (.getMetaData rs)]
    (loop [[idx & idxs] (range-sql (.getColumnCount rs-meta))
           res []]
      (if idx
        (recur idxs
               (conj res {:name
                          (.getColumnName rs-meta idx)
                          :catalog
                          (.getCatalogName rs-meta idx)
                          :display-size
                          (.getColumnDisplaySize rs-meta idx)
                          :type
                          (.getColumnType rs-meta idx)
                          :precision              (.getPrecision
                                                   rs-meta idx)
                          :scale                  (.getScale rs-meta idx)
                          :is-auto-increment
                          (.isAutoIncrement rs-meta idx)
                          :is-case-sensitive
                          (.isCaseSensitive rs-meta idx)
                          :is-currency            (.isCurrency
                                                   rs-meta idx)
                          :is-definitely-writable
                          (.isDefinitelyWritable rs-meta idx)
                          :is-nullable            (.isNullable
                                                   rs-meta idx)
                          :is-read-only           (.isReadOnly
                                                   rs-meta idx)
                          :is-searchable          (.isSearchable
                                                   rs-meta idx)
                          :is-signed              (.isSigned rs-meta idx)
                          :is-writable            (.isWritable
                                                   rs-meta idx)
                          }))
        res))))





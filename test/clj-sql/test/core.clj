(ns clj-sql.test.core
  "Tests for clj-sql as a whole"
  (:use clojure.test)
  (:import [org.h2.jdbcx JdbcConnectionPool])
  (:require [clj-sql.core :as sql]))

;;======== In memory H2 database ===============================================
(def *db*
     {:datasource (JdbcConnectionPool/create 
                   "jdbc:h2:mem:test",
                   "sa","")})

;;======== Helper functions that can be run from the repl ======================

(defn do-command
  "Runs the given sql"
  [s]
  (sql/with-connection *db*
    (sql/do-commands s)))

(defn select
  "Runs an sql select and returns the results"
  [& s]
  (sql/with-connection *db*
    (sql/with-query-results res [(apply str s)]
      (doall res))))

(defn create-tables []
  (sql/with-connection *db*
    (sql/create-table
     :test-one
     [:id :bigint "generated by default as identity"]
     [:my-txt "varchar(20)"])
    (sql/create-table
     :test-two
     [:id :bigint "generated by default as identity"]
     [:num-field :bigint]
     [:my-txt "varchar(20)"])))

(defn drop-tables
  "Drops the test tables"
  []
  (sql/with-connection *db*
    (sql/drop-table :test-one)
    (sql/drop-table :test-two)))

(defn reset
  "Drops the test table without error"
  []
  (try
    (drop-tables)
    (catch Exception _)))

;;======== Unittests ===========================================================

(defn run-test
  "Runs a unittest function f"
  [f]
  (reset)
  (create-tables)
  (sql/with-connection *db* (f)))

(use-fixtures :each run-test)

(deftest create
  ;; An exception will be thrown if no table exists
  (is (nil? (select "select * from \"test-one\""))))

(deftest insert
  (sql/insert-values :test-one [:my-txt] ["xxx"])
  (is (= (select "select id, \"my-txt\" from \"test-one\"")
         [{:id 1 :my-txt "xxx"}])))

(deftest insert-record
  (is (= 1
         (sql/insert-record :test-one {:my-txt "xxx"})))
  (is (= 2
         (sql/insert-record :test-one {:my-txt "yyy"}))))

(deftest insert-with-id
  (sql/insert-with-id
    :test-one {:my-txt "xxx"}
    :test-two {:num-field (id :test-one)
               :my-txt "yyy"})
  (is (= (select "select \"test-one\".id as one"
                 ", \"test-two\".id as two"
                 ", \"test-two\".\"num-field\" as \"num-field-two\""
                 ", \"test-one\".\"my-txt\" as \"txt-field-one\""
                 ", \"test-two\".\"my-txt\" as \"txt-field-two\""
                 " from \"test-one\", \"test-two\""
                 " where \"test-one\".id = \"test-two\".\"num-field\"")
         [{:one 1               :two 1
                                :num-field-two 1
           :txt-field-one "xxx" :txt-field-two "yyy"}])))

(deftest do-insert
  (is (= 1
         (sql/do-insert
          "insert into \"test-one\" (\"my-txt\") values (?)"
          ["xxx"])))
  (is (= 2
         (sql/do-insert
          "insert into \"test-one\" (\"my-txt\") values (?)"
          ["yyy"])))
  (is (= 3
         (sql/do-insert
          (str "insert into \"test-one\" (\"my-txt\")"
               " select ? where not exists"
               " (select id from \"test-one\" where \"my-txt\" = ?)")
          ["zzz" "zzz"])))
  (is (nil?
         (sql/do-insert
          (str "insert into \"test-one\" (\"my-txt\")"
               " select ? where not exists"
               " (select id from \"test-one\" where \"my-txt\" = ?)")
          ["zzz" "zzz"]))))
  

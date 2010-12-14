(ns clj-sql.resultset
  "Functions for dealing with resultsets"
  (:import [java.sql ResultSet]))

;;
;; A derived version of resultset-seq from clojure.core
;;
(defn as-seq
  "Creates and returns a lazy sequence of structmaps corresponding to the
rows in the java.sql.ResultSet rs. The argument keyfn is a function
to map keys from the result set (via ResultSet.getColumnLabel) to keys for the
structmap basis."
  [^java.sql.ResultSet rs keyfn]
  (let [rsmeta (. rs (getMetaData))
        idxs (range 1 (inc (. rsmeta (getColumnCount))))
        keys (map keyfn (map (fn [i] (. rsmeta (getColumnLabel i))) idxs))
        check-keys (or (apply distinct? keys) 
                       (throw (Exception. "ResultSet must have unique column labels")))
        row-struct (apply create-struct keys)
        row-values (fn [] (map (fn [^Integer i] (. rs (getObject i))) idxs))
        rows (fn thisfn [] (when (. rs (next))
                             (cons (apply struct row-struct (row-values))
                                   (lazy-seq (thisfn)))))]
    (rows)))



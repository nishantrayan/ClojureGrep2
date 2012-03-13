(ns com.rayan.clojuregrep.simple-grep
  (:import (java.io FileInputStream File InputStreamReader BufferedReader))
  (:use [clojure.java.io :only [reader writer]])
  (:gen-class true))

(defn grep-lines [lines expression]
  (filter #(re-find (re-pattern expression) %) lines))

(defn quick-and-dirty-grep2 [file-name expression]
  "Greps the file for expression and outputs a lazy sequence of lines that match them"
  (let [lines (line-seq (reader file-name))]
    (grep-lines lines expression)))

(defn write-file [file-name]
  (with-open [w (writer file-name)]
    (doseq [line (interleave (repeat 100000 "grep this line") (repeat 100000 "don't look at this line"))]
      (.write w line))))
(defn measure-time [func]
  (let [start (System/currentTimeMillis)]
    (func)
    (- (System/currentTimeMillis) start)))
(def s (measure-time #(quick-and-dirty-grep2 "something.txt" "grep")))
(println s)

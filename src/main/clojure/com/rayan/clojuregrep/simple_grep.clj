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

(defn chunk-lines [lines n]
  (loop [rem-lines lines chunked-lines []]
    (if (<= (count rem-lines) n)
      (conj chunked-lines rem-lines)
      (let [first-chunk (take n rem-lines)]
        (recur (drop n rem-lines) (conj chunked-lines first-chunk))))))

(defn grep-improved [file-name expression]
  "Improved grep"
  (let [lines (line-seq (reader file-name))
        chunked-lines (chunk-lines lines 1000)
        grep-funs (for [chunk chunked-lines] #(grep-lines chunk expression))]
    (apply pcalls grep-funs)))

(defn write-file [file-name]
  (with-open [w (writer file-name)]
    (doseq [line (interleave (repeat 100000 "grep this line") (repeat 1000000 "don't look at this line"))]
      (.write w line))))
(defn measure-time [func]
  (let [start (System/currentTimeMillis)]
    (func)
    (- (System/currentTimeMillis) start)))
;(write-file "something.txt")
(println (for [i (range 1)] (measure-time #(quick-and-dirty-grep2 "something.txt" "grep"))))
(println (count (quick-and-dirty-grep2 "something.txt" "grep")))
(println (= (quick-and-dirty-grep2 "something.txt" "grep") (grep-improved "something.txt" "grep")))
(println (count (grep-improved "something.txt" "grep")))
(println (for [i (range 1)] (measure-time #(grep-improved "something.txt" "grep"))))
(shutdown-agents)
;(println s)

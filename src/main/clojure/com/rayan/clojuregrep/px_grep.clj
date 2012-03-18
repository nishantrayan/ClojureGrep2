(ns com.rayan.clojuregrep.px-grep
  (:import (java.io FileWriter BufferedWriter
                               FileInputStream
                               File InputStreamReader BufferedReader RandomAccessFile BufferedInputStream))
  (:use [clojure.java.io :only [reader writer]])
  (:gen-class true))
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
; PX Grep is a tool that will grep for an expression in a log directory. It parallelizes the grep across files,
; making it faster than normal grep
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn reader-gz [file-name skip-bytes]
  (let [reader (-> (doto (java.util.zip.GZIPInputStream.
                           (FileInputStream. file-name))
                     (.skip skip-bytes))
    (BufferedInputStream. 131072)
    (InputStreamReader. "US-ASCII")
    (BufferedReader.))]
    reader))

(let [wtr (agent *out*)]
  (defn log [msg]
    (letfn [(write [out msg]
              (.write out msg)
              out)]
      (send-off wtr write msg))))
(defn matches-line [expression line]
  (not (nil? (re-find (re-pattern expression) line))))

(defn grep-files [files-chunk expr]
  (doseq [file files-chunk]
    (with-open [r (reader-gz file 0)]
      (let [lines (filter (partial matches-line expr) (line-seq r))]
        (doseq [line lines]
          (log (str line "\n")))))))

(defn grep-directory [directory expression]
  (let [files-seq (tree-seq #(.isDirectory %) #(.listFiles %) (File. directory))
        files-chunks (partition-all (long (/ (count files-seq) 10)) files-seq)
        grep-funcs (for [files-chunk files-chunks] #(grep-files files-chunk expression))]
    (doall (apply pmap grep-funcs))))

(grep-files ["sample_file.txt.gz"] "Clojure")
(.flush *out*)
(shutdown-agents)



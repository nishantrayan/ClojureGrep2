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
(defn reader-gz [file skip-bytes]
  (let [reader (-> (doto (java.util.zip.GZIPInputStream.
                           (FileInputStream. file))
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
          (log (str file ":" line "\n")))))))

(defn grep-directory [directory expression nthreads]
  (let [files-seq (filter #(not (.isDirectory %)) (tree-seq #(.isDirectory %) #(.listFiles %) (File. directory)))
        files-chunks (partition-all (long (/ (count files-seq) nthreads)) files-seq)
        grep-funcs (for [files-chunk files-chunks] #(grep-files files-chunk expression))]
    (doall (apply pcalls grep-funcs))))

;(grep-files ["sample_file.txt.gz"] "Clojure")
(def dir (nth *command-line-args* 0))
(def expr (nth *command-line-args* 1))
(def nthreads (Integer/parseInt (nth *command-line-args* 2)))
;(println dir expr nthreads)
(defn measure-time [func]
  (let [start (System/currentTimeMillis)]
    (func)
    (- (System/currentTimeMillis) start)))
(def t (measure-time #(grep-directory dir expr nthreads)))
(log (str "it took " t " ms\n"))
(await)
(shutdown-agents)



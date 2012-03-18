(ns com.rayan.clojuregrep.simple-grep
  (:import (java.io FileInputStream File InputStreamReader BufferedReader RandomAccessFile BufferedInputStream))
  (:use [clojure.java.io :only [reader writer]])
  (:gen-class true))

(defn grep-lines [lines expression]
  (filter #(re-find (re-pattern expression) %) lines))

(defn matches-line [line expression]
  (not (nil? (re-find (re-pattern expression) line))))


(defn read-lines-range [file select-line-func start-byte end-byte]
  "Returns a lazy sequence of lines from file between start-byte and end-byte."
  (let [reader (-> (doto (FileInputStream. file)
                     (.skip start-byte))
    (BufferedInputStream. 131072)
    (InputStreamReader. "US-ASCII")
    (BufferedReader.))]
    (letfn [(gobble-lines [remaining]
              (lazy-seq
                (if-let [line (and (pos? remaining) (.readLine reader))]
                  (if (select-line-func line)
                    (cons line (gobble-lines (- remaining (inc (.length line)))))
                    (gobble-lines (- remaining (inc (.length line)))))
                  (.close reader))))]
      (gobble-lines (- end-byte start-byte)))))
(defn chunk-file
  "Partitions a file into n line-aligned chunks.  Returns a list of start and
  end byte offset pairs."
  [filename n]
  (with-open [file (RandomAccessFile. filename "r")]
    (let [offsets (for [offset (range 0 (.length file) (/ (.length file) n))]
      (do
        (when-not (zero? offset)
          (.seek file offset)
          (while (not= (.read file) (int \newline))))
        (.getFilePointer file)))
          offsets (concat offsets [(.length file)])]
      (doall (partition 2 (interleave offsets (rest offsets)))))))

(defn quick-and-dirty-grep2 [file-name expression]
  "Greps the file for expression and outputs a lazy sequence of lines that match them"
  (let [lines (line-seq (reader file-name))]
    (grep-lines lines expression)))

(defn chunk-lines [lines n]
  (lazy-seq
    (loop [rem-lines lines chunked-lines []]
      (if (<= (count rem-lines) n)
        (conj chunked-lines rem-lines)
        (let [first-chunk (take n rem-lines)]
          (recur (drop n rem-lines) (conj chunked-lines first-chunk)))))))
;(defn my-flat [sq]
;  (loop [f [] rem-sq sq]
;    (if (zero? (count sq))
;      f
;      (recur (concat f (first sq)))))
(defn grep-improved [file-name expression]
  "Improved grep"
  (let [chunked-bytes (chunk-file file-name 10)]
    (let [grep-funs (for [chunk chunked-bytes] #(read-lines-range file-name (fn [l] (matches-line l expression)) (first chunk) (last chunk)))]
      (apply pcalls grep-funs))))

(defn write-file [file-name]
  (with-open [w (writer file-name)]
    (doseq [line (interleave (repeat 100000 "grep this line") (repeat 100000 "don't look at this line"))]
      (.write w line)
      (.write w "\n"))))
(defn measure-time [func]
  (let [start (System/currentTimeMillis)]
    (func)
    (- (System/currentTimeMillis) start)))
(write-file "something.txt")
(println (for [i (range 10)] (measure-time #(doall (quick-and-dirty-grep2 "something.txt" "Clojure")))))
;(println (quick-and-dirty-grep2 "something.txt" "grep"))

(println (= (quick-and-dirty-grep2 "something.txt" "Clojure") (flatten (grep-improved "something.txt" "Clojure"))))
;(println (grep-improved "something.txt" "grep"))
(println (for [i (range 10)] (measure-time #(doall (grep-improved "something.txt" "Clojure")))))
(shutdown-agents)
;(println s)

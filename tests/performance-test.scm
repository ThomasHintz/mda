(use mda-client tokyocabinet srfi-18 ports)

(define db (make-parameter #f))

(define (setup-db)
  (db (tc-hdb-open "ktr-db" flags:
                   (fx+ TC_HDBOWRITER (fx+ TC_HDBOREADER TC_HDBOCREAT)))))

(setup-db)

(define (test-raw-read-proc)
  ;(store (db) "k" "v")
  (tc-hdb-get (db) "k"))

(define (test-mda-read-proc)
  ;(db:store "v" "k")
  (db:read "k"))

(define (test-raw-store-proc)
  (tc-hdb-put! (db) "k" "v"))

(define (test-mda-store-proc)
  (db:store "v" "k"))

(define (do-times num proc)
  (letrec ((_ (lambda (i num proc)
                (if (> i num)
                    #t
                    (begin (proc)
                           (_ (+ i 1) num proc))))))
    (_ 0 num proc)))

(define num-test-runs 100)
(define num-threads 100)

(define (do-raw-store-test)
  (do-times num-test-runs test-raw-store-proc))

(define (do-mda-store-test)
  (do-times num-test-runs test-mda-store-proc))

(define (do-raw-read-test)
  (do-times num-test-runs test-raw-read-proc))

(define (do-mda-read-test)
  (do-times num-test-runs test-mda-read-proc))


(define (range from/to . to)
  (let ((f (if (= (length to) 0) -1 (- from/to 1)))
        (t (if (> (length to) 0) (first to) from/to)))
    (do ((i (- t 1) (- i 1))
         (l '() (cons i l)))
        ((= i f) l))))

(define (run-test proc)
  (map (lambda (thread)
         (thread-join! thread))
       (map (lambda (arg-not-used)
              (let ((t (make-thread proc)))
                (thread-start! t)
                t))
            (range num-threads))))

(print "raw read time")
;(time (run-test do-raw-read-test))
(print "mda read time")
(time (run-test do-mda-read-test))
(print "raw store time")
;(time (run-test do-raw-store-test))
(print "mda store time")
(time (run-test do-mda-store-test))

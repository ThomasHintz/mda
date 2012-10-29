(use mda-client srfi-18 ports)

(define (test-proc)
  (db:store "v" "k")
  (db:read "k"))



(define (do-times num proc)
  (letrec ((_ (lambda (i num proc)
                (if (> i num)
                    #t
                    (begin (proc)
                           (_ (+ i 1) num proc))))))
    (_ 0 num proc)))

(define (do-test)
  (do-times 1000 test-proc))


(define (range from/to . to)
  (let ((f (if (= (length to) 0) -1 (- from/to 1)))
        (t (if (> (length to) 0) (first to) from/to)))
    (do ((i (- t 1) (- i 1))
         (l '() (cons i l)))
        ((= i f) l))))

(map (lambda (thread)
       (thread-join! thread))
     (map (lambda (arg-not-used)
            (let ((t (make-thread do-test)))
              (thread-start! t)
              t))
          (range 1000)))

(print "success")

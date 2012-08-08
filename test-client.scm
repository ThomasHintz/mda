(use zmq)

(define socket (make-socket 'req))
(connect-socket socket "tcp://localhost:4444")

(define (serialize l) (with-output-to-string (lambda () (write l))))
(define (deserialize s) (with-input-from-string s (lambda () (read))))

(define (store-test)
  (send-message socket (serialize '(put "v" "k")))
  (receive-message* socket))

(define (read-test)
  (send-message socket (serialize '(get "k")))
  (receive-message* socket))

;(store-test)
;(read-test)

(define (do-op op)
  (send-message socket (serialize op))
  (deserialize (receive-message* socket)))

(define (put v . k)
  (do-op `(put ,v ,@k)))

(define (get k)
  (do-op `(get ,k)))

(define (many n proc)
  (letrec ((process (lambda (i) (if (> i n) 'done (begin (proc) (process (+ i 1)))))))
    (process 0)))

(define (perf-test n) (many n (lambda () (put "v" "k") (get "k"))))
(define (perf-test2 n) (many n (lambda () (do-op '(+ 1 2)))))

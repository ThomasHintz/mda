(use tokyocabinet http-client intarweb uri-common)

(define *db* (tc-hdb-open "test-db2"))
(define *my-ip/port* "192.168.43.144:9002")
(define *master-ip/port* "192.168.144:9001")
(define *server-list* '())
(define *slave-list* '())

(define-res 'PUT '("data" key)
  (lambda (value key)
    (tc-hdb-put! *db* key (->string value))
    (->string value)))

(define-res 'GET '("data" key)
  (lambda (key)
    (->string (tc-hdb-get *db* key))))

(define-res 'DELETE '("data" key)
  (lambda (key)
    (tc-hdb-delete! *db* key)))

(define (server-call method location . data)
  (with-input-from-request (make-request method: method uri: (uri-reference location)) (if (> (length data) 0) (car data) #f) read-string))

(define (get-server-list loc)
  (handle-exceptions exn '() (with-input-from-string (server-call 'GET (string-append "http://" loc "/server-list")) read)))

(define (setup-server-list my-ip/port)
  (set! *server-list* (append (get-server-list *master-ip/port*) (list my-ip/port)))
  (if (> (length *server-list*) 1)
      #f ; i am a slave!
      #f ; i am the master!
      ))
(setup-server-list *my-ip/port*)

(define-res 'GET '("server-list")
  (lambda ()
    (->string *server-list*)))

(define-res 'PUT '("server-list")
  (lambda (server-list)
    (set! *server-list* (with-input-from-string server-list read))
    ""))

(define-res 'PUT ("slave")
  (lambda (slave-ip/port)
    (set! *slave-list* (cons my-ip/port (with-input-from-string slave-ip/port read)))))

(define-res 'DELETE ("slave")
  (lambda (slave-ip/port)
    'un-register-slave))

(define-res 'GET ("database")
  (lambda ()
    'the-database))
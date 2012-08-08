(use srfi-1 srfi-13 srfi-18 srfi-69 tokyocabinet zmq ports data-structures)
(include "mda-common.scm")

;;; utils

(define (dash->space s)
  (string-fold (lambda (c o) (string-append o (if (char=? #\- c) " " (->string c)))) "" s))

(define (space->dash s)
  (string-fold (lambda (c o) (string-append o (if (char=? #\space c) "-" (->string c)))) "" s))

(define (id->name id)
  (string-titlecase (dash->space id)))

(define (name->id name)
  (string-downcase (space->dash name)))

(define (list->path list)
  (fold (lambda (e o)
          (string-append o (sep) e))
        ""
        list))

(define db (make-parameter #f))

(define (open-db path)
  (tc-hdb-open path))

;;; setup tokyocabinet

(define db-path (make-parameter ""))

(define *sep* "/")
(define (sep . val)
  (if (> (length val) 0)
      (set! *sep* (first val))
      *sep*))

(define list-index (make-parameter "the-list-index"))

(define (contains? l e)
  (not (eq? (filter (lambda (le) (string=? le e)) l) '())))

;;; macros

(define-syntax db+
  (syntax-rules ()
    ((db+ amount first ...) (db:store (+ (db:read first ...) amount) first ...))))

(define-syntax db-
  (syntax-rules ()
    ((db- amount first ...) (db:store (- (db:read first ...) amount) first ...))))

;;; tokyocabinet db operations

(define (tc-store db data path-list)
  (let ((k (name->id (list->path path-list)))
	(v (with-output-to-string (lambda () (write data)))))
    (tc-hdb-put! db k v)))

(define (tc-read db path-list)
  (let ((val (tc-hdb-get db (name->id (list->path path-list)))))
    (if val
        (with-input-from-string val (lambda () (read)))
        'not-found)))

(define (tc-delete db path-list)
  (let ((k (name->id (list->path path-list))))
    (tc-hdb-delete! db k)))

(define (tc-exists? db path-list)
  (not (eq? (tc-read db path-list) 'not-found)))

;;; external funcs, they wrap db calls with permission and locking protection

(define (put data . path-list)
  (tc-store (db) (deserialize data) path-list)
  'success)

(define (get . path-list)
  (tc-read (db) path-list))

(define (list-old . path-list)
  (let* ((s-form (list->path path-list))
	 (s-length (string-length s-form))
	 (list-length (length path-list)))
    (delete-duplicates (pair-fold (db:db)
				  (lambda (k v kvs)
				    (if (string= s-form k 0 s-length 0 (if (< (string-length k) s-length) 0 s-length))
					(cons (list-ref (string-split k (db:sep)) list-length) kvs)
					kvs))
				  '())
		       string=)))

(define (update-list data . path-list)
  (let* ((p (append path-list `(,(list-index))))
	 (l (tc-read (db) p))
	 (ls (if (eq? l 'not-found) '() l)))
    (or (contains? ls data) (tc-store (db) (cons data ls) p))))

(define (db-list . path-list)
  (let ((r (tc-read (db) (append path-list `(,(list-index))))))
    (if (eq? r 'not-found)
	'()
	r)))

(define (delete . path-list)
  (tc-delete (db) path-list))

(define (delete-r . path-list)
  (let* ((s-form (list->path path-list))
	 (s-length (string-length s-form))
	 (list-length (length path-list)))
    (map (lambda (k) (delete! (db:db) k))
	 (pair-fold (db:db)
		    (lambda (k v kvs)
		      (if (string= s-form k 0 s-length 0 (if (< (string-length k) s-length) 0 s-length))
			  (cons k kvs)
			  kvs))
		    '()))))

;;; setup the server

(db (open-db (car (command-line-arguments))))

(define socket (make-parameter (make-socket 'rep)))
(bind-socket (socket) "tcp://*:4444")

(define (process-request)
  (handle-exceptions
   exn
   (send-message (socket) (serialize `(error ,(with-output-to-string (lambda ()
								       (print-call-chain)
								       (print-error-message exn))))))
   (let ((msg (serialize `(success ,(eval (deserialize (receive-message* (socket))))))))
     (send-message (socket) msg)))
  (process-request))

(process-request)

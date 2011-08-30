#!/usr/bin/csi -script

(require-extension spiffy intarweb srfi-69 uri-common)

(define ++ string-append)

(define (concat args #!optional (sep ""))
    (string-intersperse (map ->string args) sep))

(define *resources* (make-hash-table equal?))
(define res-headers (lambda (res) `((content-type text/x-scheme) (content-length ,(string-length res)))))

(define (request-body)
  (let* ((headers (request-headers (current-request)))
	 (content-length (header-value 'content-length headers)))
    (read-string content-length (request-port (current-request)))))

(define (register-dispatcher)
  (handle-not-found
   (let ((old-handler (handle-not-found)))
     (lambda (p)
       (let* ((path (++ "/" (concat (cdr (uri-path (request-uri (current-request)))) "/")))
	      (proc/vars (resource-ref (cons (symbol->string (request-method (current-request))) (string-split path "/")) *resources* '()))
	      (proc (car proc/vars))
	      (vars-temp (car (cdr proc/vars)))
	      (vars (if (eq? (request-method (current-request)) 'PUT) (cons (request-body) vars-temp) vars-temp)))
         (if proc
             (run-resource proc path vars)
             (old-handler path)))))))

(register-dispatcher)

(define (add-resource! path proc ht)
  (if (string? (car path)) ; should we do static stuff or variable stuff?
      (if (> (length (cdr path)) 0) ; if this is not the last element of the path, the last element means we store a proc instead of a hashtable
	  (let ((next-ht (hash-table-ref/default ht (car path) #f)))
	    (if (hash-table? next-ht)
		#f
		(set! next-ht (make-hash-table equal?)))
	    (hash-table-set! ht (car path) next-ht)
	    (add-resource! (cdr path) proc next-ht))
	  (hash-table-set! ht (car path) proc))
      (if (> 0 (length (cdr path)))
	  (let ((next-ht (hash-table-ref/default ht (car path) #f)))
	    (if (hash-table? next-ht)
		#f
		(set! next-ht (make-hash-table equal?)))
	    (hash-table-set! ht "/" next-ht)
	    (add-resource! (cdr path) proc next-ht))
	  (hash-table-set! ht "/" proc))))

; checks the hash table to see if the key exists and if it does just returns the corresponding value
; otherwise checks the special key /, which is used if the key is variable. If the / key exists then it returns a variable signifier and the value of the / key
(define (hash-table-or-var key ht)
  (let ((v (hash-table-ref/default ht key #f)))
    (if v v (let ((v (hash-table-ref/default ht "/" #f)))
	      (if v `(var ,v) #f)))))

; if the resource exists, returns a list with the car being a proc and the cdr being the variable list values
; ex: (proc ("asdf" "qwerty"))
(define (resource-ref path ht var-list)
  (let ((res (hash-table-or-var (car path) ht)))
    (if (hash-table? res)
	(resource-ref (cdr path) res var-list)
	(if (and (list? res) (eq? (car res) 'var))
	    (if (hash-table? (cdr res))
		(resource-ref (cdr path) (cdr res) (cons (car path) var-list))
		`(,(car (cdr res)) ,(reverse (cons (car path) var-list))))
	    `(,res ,(reverse var-list))))))

(define (run-resource proc path vars)
  (let ((res (apply proc vars)))
    (with-headers (res-headers res)
                  (lambda ()
                    (write-logged-response)
                    (request-method (current-request))
                    (display res (response-port (current-response)))))))

(define (define-res method path proc)
  (add-resource! (cons (->string method) path) proc *resources*))

(define (load-resources res-list)
  (for-each load res-list))

(load-resources '("mda.scm"))

(parameterize ((server-port 9002))
	      (start-server))
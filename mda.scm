; author: Thomas Hintz
; email: t@thintz.com
; license: bsd

; Copyright (c) 2012, Thomas Hintz
; All rights reserved.

; Redistribution and use in source and binary forms, with or without
; modification, are permitted provided that the following conditions are met:
;     * Redistributions of source code must retain the above copyright
;       notice, this list of conditions and the following disclaimer.
;     * Redistributions in binary form must reproduce the above copyright
;       notice, this list of conditions and the following disclaimer in the
;       documentation and/or other materials provided with the distribution.
;     * Neither the name of the <organization> nor the
;       names of its contributors may be used to endorse or promote products
;       derived from this software without specific prior written permission.

; THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
; ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
; WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
; DISCLAIMED. IN NO EVENT SHALL THOMAS HINTZ BE LIABLE FOR ANY
; DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
; (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
; 	    LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
; 	    ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
; 	    (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
; 	    SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

#!/usr/bin/csi -script

(use srfi-1 srfi-13 srfi-18 srfi-69 tokyocabinet zmq)

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
  (tc-store (db) data path-list)
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

(define (list . path-list)
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

;(print (store "v2" "k2"))
;(print (read "k2"))

(define (many n proc)
  (letrec ((process (lambda (i) (if (> i n) 'done (begin (proc) (process (+ i 1)))))))
    (process 0)))

(define (perf-test n) (many n (lambda () (put "v" "k") (get "k"))))
(define (perf-test2 n) (many n (lambda () (+ 1 2))))

(print (time (perf-test2 10000)))

(define (process-request)
  (let ((msg (receive-message* (socket))))
    (let ((proc (with-input-from-string msg (lambda () (read)))))
      (send-message (socket) (with-output-to-string (lambda () (write (eval proc)))))))
  (process-request))

(process-request)

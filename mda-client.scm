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

(module mda-client
  (;; params
   db:sep

   ;; procs
   db:store db:read db:list db:update-list db:remove-from-list db:delete
   db:pause
   )

(import scheme chicken ports srfi-13 data-structures)
(use tokyocabinet srfi-1 srfi-13 srfi-18)

;;; utils

(define *sep* "/")
(define (sep . val)
  (if (> (length val) 0)
      (set! *sep* (first val))
      *sep*))

(define list-index (make-parameter "the-list-index"))

(define (contains? l e)
  (not (eq? (filter (lambda (le) (string=? le e)) l) '())))

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

;;; db funcs

(define db (make-parameter #f))
(setup-db)

(define (setup-db)
  (db (tc-hdb-open "ktr-db" flags:
                   (fx+ TC_HDBONOLCK (fx+ TC_HDBOWRITER (fx+ TC_HDBOREADER TC_HDBOCREAT))))))

(define db:sep (make-parameter "/"))

(define (db:store data . path-list)
  (when (not (db)) (setup-db))
  (let ((k (name->id (list->path path-list)))
	(v (with-output-to-string (lambda () (write data)))))
    (tc-hdb-put! (db) k v) #t))

(define (db:read . path-list)
  (when (not (db)) (setup-db))
  (let ((val (tc-hdb-get (db) (name->id (list->path path-list)))))
    (if val
        (with-input-from-string val (lambda () (read)))
        'not-found)))

(define (db:list . path-list)
  (let ((r (apply db:read (append path-list `(,(list-index))))))
    (if (eq? r 'not-found)
	'()
	r)))

(define (db:update-list data . path-list)
  (let* ((p (append path-list `(,(list-index))))
	 (l (apply db:read p))
	 (ls (if (eq? l 'not-found) '() l)))
    (or (contains? ls data) (apply db:store (cons data ls) p))))

(define (db:remove-from-list data . path-list)
  (let* ((p (append path-list `(,(list-index))))
	 (l (apply db:read p))
	 (ls (if (eq? l 'not-found) '() l)))
    (apply db:store
           (filter (lambda (e)
                     (not (equal? e data)))
                   ls)
           p)))

(define (db:delete . path-list)
  (when (not (db)) (setup-db))
  (let ((k (name->id (list->path path-list))))
    (tc-hdb-delete! (db) k)))

(define (db:pause)
  'not-implemented)
  ;(do-op `(pause)))

)

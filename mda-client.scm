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
   retries timeout db:sep

   ;; procs
   db:store db:put-async db:read db:list db:update-list db:remove-from-list db:delete
   serialize deserialize
   )

(import scheme chicken ports)
(use zmq)

(include "mda-common.scm")

(define retries (make-parameter 15)) ; with 100ms timeout this makes the max timeout 1.5s
(define timeout (make-parameter (* 1000 1000))) ; 100ms

(define socket #f)

(define (setup-socket)
  (set! socket (make-socket 'req))
  (connect-socket socket "tcp://localhost:4444"))
(setup-socket)

(define (reconnect)
  (close-socket socket)
  (setup-socket))

(define (do-op op)
  (send-message socket (serialize op))
  (letrec ((poll-loop
	    (lambda (timeout retries max-retries)
	      (if (>= retries max-retries)
		  (abort 'db-connection-timeout)
		  (let ((pi `(,(make-poll-item socket in: #t))))
		    (if (= 0 (poll pi timeout))
			(begin (reconnect)
			       (poll-loop timeout (+ retries 1) max-retries))
			(let ((response (deserialize (receive-message* socket))))
			  (if (eq? (car response) 'success)
			      (cadr response)
			      (begin (print response) (abort (cdr response)))))))))))
	   (poll-loop (timeout) 0 (retries))))

(define db:sep (make-parameter "/"))

(define (db:store v . k)
  (do-op `(put ,(serialize v) ,@k)))

(define (db:put-async v . k) 'not-implemented)

(define (db:read . k)
  (do-op `(get ,@k)))

(define (db:list . k)
  (do-op `(db-list ,@k)))

(define (db:update-list v . k)
  (do-op `(update-list ,v ,@k)))

(define (db:remove-from-list v . k)
  (do-op `(remove-from-list ,v ,@k)))

(define (db:delete . k)
  (do-op `(delete ,@k)))

)

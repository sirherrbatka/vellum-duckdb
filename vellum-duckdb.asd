(asdf:defsystem #:vellum-duckdb
  :name "vellum-duckdb"
  :description "Use duckdb features with Vellum!"
  :license "BSD simplified"
  :author "Marek Kochanowicz"
  :depends-on ( #:iterate
                #:serapeum
                (:version #:vellum ((>= "1.3.4")))
                #:alexandria
                #:documentation-utils-extensions
                #:duckdb)
  :serial T
  :pathname "src"
  :components ((:file "package")
               (:file "code")))

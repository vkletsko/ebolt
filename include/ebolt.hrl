-author("Vitali Kletsko <v.kletsko@gmail.com>").

-record(error, {
  level :: fatal | error | atom(),
  code :: binary(),
  codename :: atom(),
  message :: binary()
}).
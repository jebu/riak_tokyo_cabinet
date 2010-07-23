{application, toke,
 [
  {description, ""},
  {vsn, "1.0"},
  {modules, [
             toke_drv,
             toke_test
            ]},
  {registered, []},
  {applications, [
                  kernel,
                  stdlib
                 ]},
  {mod, { toke_app, []}},
  {env, []}
 ]}.

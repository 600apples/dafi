from daffi import Router


router1 = Router(host="0.0.0.0", port=5000)
router1.start()
router1.join()
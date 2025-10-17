package tracker

import "errors"

var errZeroValue = errors.New("zero value provided")

var errNilWSClient = errors.New("nil sui ws client")

var errNilRPCClient = errors.New("nil sui rpc client")

var errNilIncomingHeadersCreator = errors.New("nil sui incoming headers creator")

var errNilHeadersNotifier = errors.New("nil sui headers notifier")

var errNilStorer = errors.New("nil nonce storer for SUI tracker")

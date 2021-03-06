import createDebug from 'debug'
const debug = createDebug('xo:api')

import assign from 'lodash.assign'
import Bluebird from 'bluebird'
import forEach from 'lodash.foreach'
import getKeys from 'lodash.keys'
import isFunction from 'lodash.isfunction'
import map from 'lodash.map'
import schemaInspector from 'schema-inspector'

import {
  InvalidParameters,
  JsonRpcError,
  MethodNotFound,
  NoSuchObject,
  Unauthorized
} from './api-errors'

// ===================================================================

// FIXME: this function is specific to XO and should not be defined in
// this file.
function checkPermission (method) {
  /* jshint validthis: true */

  const {permission} = method

  // No requirement.
  if (permission === undefined) {
    return
  }

  const {user} = this
  if (!user) {
    throw new Unauthorized()
  }

  // The only requirement is login.
  if (!permission) {
    return
  }

  if (!user.hasPermission(permission)) {
    throw new Unauthorized()
  }
}

// -------------------------------------------------------------------

function checkParams (method, params) {
  const schema = method.params
  if (!schema) {
    return
  }

  const result = schemaInspector.validate({
    type: 'object',
    properties: schema
  }, params)

  if (!result.valid) {
    throw new InvalidParameters(result.error)
  }
}

// -------------------------------------------------------------------

// Forward declaration.
let checkAuthorization

function authorized () {}
// function forbiddden () {
//   // We don't care about an error object.
//   /* eslint no-throw-literal: 0 */
//   throw null
// }
function checkMemberAuthorization (member) {
  return function (userId, object, permission) {
    const memberObject = this.getObject(object[member])
    return checkAuthorization.call(this, userId, memberObject, permission)
  }
}

const checkAuthorizationByTypes = {
  // Objects of these types do not requires any authorization.
  'network': authorized,
  'VM-template': authorized,

  message: checkMemberAuthorization('$object'),

  task: checkMemberAuthorization('$host'),

  VBD: checkMemberAuthorization('VDI'),

  // Access to a VDI is granted if the user has access to the
  // containing SR or to a linked VM.
  VDI (userId, vdi, permission) {
    // Check authorization for each of the connected VMs.
    const promises = map(this.getObjects(vdi.$VBDs, 'VBD'), vbd => {
      const vm = this.getObject(vbd.VM, 'VM')
      return checkAuthorization.call(this, userId, vm, permission)
    })

    // Check authorization for the containing SR.
    const sr = this.getObject(vdi.$SR, 'SR')
    promises.push(checkAuthorization.call(this, userId, sr, permission))

    // We need at least one success
    return Bluebird.any(promises)
  },

  VIF (userId, vif, permission) {
    const network = this.getObject(vif.$network)
    const vm = this.getObject(vif.$VM)

    return Bluebird.any([
      checkAuthorization.call(this, userId, network, permission),
      checkAuthorization.call(this, userId, vm, permission)
    ])
  },

  'VM-snapshot': checkMemberAuthorization('$snapshot_of')
}

function throwIfFail (success) {
  if (!success) {
    // We don't care about an error object.
    /* eslint no-throw-literal: 0 */
    throw null
  }
}

function defaultCheckAuthorization (userId, object, permission) {
  return this.hasPermission(userId, object.id, permission).then(throwIfFail)
}

checkAuthorization = Bluebird.method(function (userId, object, permission) {
  const fn = checkAuthorizationByTypes[object.type] || defaultCheckAuthorization
  return fn.call(this, userId, object, permission)
})

function resolveParams (method, params) {
  const resolve = method.resolve
  if (!resolve) {
    return params
  }

  const {user} = this
  if (!user) {
    throw new Unauthorized()
  }

  const userId = user.get('id')
  const isAdmin = this.user.hasPermission('admin')

  const promises = []
  forEach(resolve, ([param, types, permission = 'administrate'], key) => {
    const id = params[param]
    if (id === undefined) {
      return
    }

    const object = this.getObject(params[param], types)

    // This parameter has been handled, remove it.
    delete params[param]

    // Register this new value.
    params[key] = object

    if (!isAdmin) {
      promises.push(checkAuthorization.call(this, userId, object, permission))
    }
  })

  return Bluebird.all(promises).catch(() => {
    throw new Unauthorized()
  }).return(params)
}

// ===================================================================

function getMethodsInfo () {
  const methods = {}

  forEach(this.api._methods, function (method, name) {
    this[name] = assign({}, {
      description: method.description,
      params: method.params || {},
      permission: method.permission
    })
  }, methods)

  return methods
}
getMethodsInfo.description = 'returns the signatures of all available API methods'

// -------------------------------------------------------------------

const getVersion = () => '0.1'
getVersion.description = 'API version (unstable)'

// -------------------------------------------------------------------

function listMethods () {
  return getKeys(this.api._methods)
}
listMethods.description = 'returns the name of all available API methods'

// -------------------------------------------------------------------

function methodSignature ({method: name}) {
  const method = this.api.getMethod(name)

  if (!method) {
    throw new NoSuchObject()
  }

  // Return an array for compatibility with XML-RPC.
  return [
    // XML-RPC require the name of the method.
    assign({ name }, {
      description: method.description,
      params: method.params || {},
      permission: method.permission
    })
  ]
}
methodSignature.description = 'returns the signature of an API method'

// ===================================================================

export default class Api {
  constructor ({context} = {}) {
    this._methods = Object.create(null)
    this.context = context

    this.addMethods({
      system: {
        getMethodsInfo,
        getVersion,
        listMethods,
        methodSignature
      }
    })
  }

  addMethod (name, method) {
    this._methods[name] = method
  }

  addMethods (methods) {
    let base = ''
    forEach(methods, function addMethod (method, name) {
      name = base + name

      if (isFunction(method)) {
        this.addMethod(name, method)
        return
      }

      const oldBase = base
      base = name + '.'
      forEach(method, addMethod, this)
      base = oldBase
    }, this)
  }

  async call (session, name, params) {
    debug('%s(...)', name)

    const method = this.getMethod(name)
    if (!method) {
      throw new MethodNotFound(name)
    }

    const context = Object.create(this.context)
    context.api = this // Used by system.*().
    context.session = session

    // FIXME: too coupled with XO.
    // Fetch and inject the current user.
    const userId = session.get('user_id', undefined)
    if (userId) {
      context.user = await context._getUser(userId)
    }

    try {
      await checkPermission.call(context, method)
      checkParams(method, params)

      await resolveParams.call(context, method, params)

      let result = await method.call(context, params)

      // If nothing was returned, consider this operation a success
      // and return true.
      if (result === undefined) {
        result = true
      }

      debug('%s(...) → %s', name, typeof result)

      return result
    } catch (error) {
      if (error instanceof JsonRpcError) {
        debug('Error: %s(...) → %s', name, error)
      } else {
        console.error(error && error.stack || error)
      }

      throw error
    }
  }

  getMethod (name) {
    return this._methods[name]
  }
}

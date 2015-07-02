import Bluebird from 'bluebird'
import filter from 'lodash.filter'
import forEach from 'lodash.foreach'
import fs from 'fs-extra'
import startsWith from 'lodash.startswith'
import {exec} from 'child_process'

const execAsync = Bluebird.promisify(exec)
Bluebird.promisifyAll(fs)

const noop = () => {}

class NfsMounter {
  async _loadRealMounts () {
    let stdout
    try {
      [stdout] = await execAsync('findmnt -P -t nfs,nfs4 --output SOURCE,TARGET --noheadings')
    } catch (exc) {
      // When no mounts are found, the call pretends to fail...
    }
    const mounted = {}
    if (stdout) {
      const regex = /^SOURCE="([^:]*):(.*)" TARGET="(.*)"$/
      forEach(stdout.split('\n'), m => {
        if (m) {
          const match = regex.exec(m)
          mounted[match[3]] = {
            host: match[1],
            share: match[2]
          }
        }
      })
    }
    this._realMounts = mounted
    return mounted
  }

  _fullPath (path) {
    return '/tmp/xo-server/mounts/' + path
  }

  _matchesRealMount (mount) {
    return this._fullPath(mount.path) in this._realMounts
  }

  async _mount (mount) {
    const path = this._fullPath(mount.path)
    await fs.ensureDirAsync(path)
    return await execAsync(`mount -t nfs ${mount.host}:${mount.share} ${path}`)
  }

  async forget (mount) {
    try {
      await this._umount(mount)
    } catch (_) {
      // We have to go on...
    }
  }

  async _umount (mount) {
    const path = this._fullPath(mount.path)
    await execAsync(`umount ${path}`)
  }

  async sync (mount) {
    await this._loadRealMounts()
    if (this._matchesRealMount(mount) && !mount.enabled) {
      try {
        await this._umount(mount)
      } catch (exc) {
        mount.enabled = true
        mount.error = exc.message
      }
    } else if (!this._matchesRealMount(mount) && mount.enabled) {
      try {
        await this._mount(mount)
      } catch (exc) {
        mount.enabled = false
        mount.error = exc.message
      }
    }
    return mount
  }

  async disableAll (mounts) {
    await this._loadRealMounts()
    forEach(mounts, async mount => {
      if (this._matchesRealMount(mount)) {
        try {
          await this._umount(mount)
        } catch(_) {
          // We have to go on...
        }
      }
    })
  }
}

class LocalHandler {
  constructor () {
    this.forget = noop
    this.disableAll = noop
  }

  async sync (local) {
    if (local.enabled) {
      await fs.ensureDirAsync(local.path)
    }
  }
}

export default class RemoteHandler {
  constructor () {
    this.handlers = {
      nfs: new NfsMounter(),
      local: new LocalHandler()
    }
  }

  _resolve (remote) {
    if (startsWith(remote.url, 'nfs://')) {
      remote.type = 'nfs'
      const url = remote.url.slice(6)
      const [host, share] = url.split(':')
      remote.path = remote.id
      remote.host = host
      remote.share = share
    } else if (startsWith('file://')) {
      remote.type = 'local'
      remote.path = remote.url.slice(6)
    } else {
      throw new Error('Unknown remote url protocol')
    }
  }

  async sync (remote) {
    return await this.handlers[this._resolve(remote).type].sync(remote)
  }

  async forget (remote) {
    return await this.handlers[this._resolve(remote).type].forget(remote)
  }

  async disableAll (remotes) {
    forEach(remotes, remote => this._resolve(remote))
    const promises = []
    forEach(['local', 'nfs'], type => promises.push(this.handlers[type].disableAll(filter(remotes, remote => remote.type === type))))
    await Promise.all(promises)
  }
}

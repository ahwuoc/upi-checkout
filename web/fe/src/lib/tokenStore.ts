/**
 * Lưu danh sách token vào **IndexedDB**.
 *
 * Vì sao không dùng localStorage: quota chỉ ~5 MB. Một batch 6.000 JWT (~2 KB mỗi cái) là ~12 MB
 * -> `setItem` ném QuotaExceededError, và vì lỗi đó mà cả cấu hình (proxy, workers…) cũng không
 * được ghi -> reload mất sạch. IndexedDB cho quota lớn hơn nhiều (theo dung lượng đĩa) và lưu
 * được chuỗi vài chục MB.
 *
 * Không dùng thư viện: chỉ cần 1 object store, 1 key.
 */
const DB_NAME = 'upi-desk'
const STORE = 'prefs'
const DB_VERSION = 1

function openDb(): Promise<IDBDatabase | null> {
  return new Promise((resolve) => {
    if (typeof indexedDB === 'undefined') {
      resolve(null)
      return
    }
    const request = indexedDB.open(DB_NAME, DB_VERSION)
    request.onupgradeneeded = () => {
      const db = request.result
      if (!db.objectStoreNames.contains(STORE)) db.createObjectStore(STORE)
    }
    request.onsuccess = () => resolve(request.result)
    request.onerror = () => resolve(null)
    request.onblocked = () => resolve(null)
  })
}

async function withStore<T>(
  mode: IDBTransactionMode,
  run: (store: IDBObjectStore) => IDBRequest<T>,
): Promise<T | null> {
  const db = await openDb()
  if (!db) return null
  return new Promise((resolve) => {
    try {
      const tx = db.transaction(STORE, mode)
      const request = run(tx.objectStore(STORE))
      request.onsuccess = () => resolve(request.result ?? null)
      request.onerror = () => resolve(null)
      tx.oncomplete = () => db.close()
      tx.onabort = () => {
        db.close()
        resolve(null)
      }
    } catch {
      db.close()
      resolve(null)
    }
  })
}

export async function loadTokens(): Promise<string> {
  const value = await withStore<string>('readonly', (store) => store.get('tokens'))
  return typeof value === 'string' ? value : ''
}

export async function saveTokens(value: string): Promise<boolean> {
  if (!value) {
    await withStore('readwrite', (store) => store.delete('tokens'))
    return true
  }
  const result = await withStore<IDBValidKey>('readwrite', (store) => store.put(value, 'tokens'))
  return result !== null
}

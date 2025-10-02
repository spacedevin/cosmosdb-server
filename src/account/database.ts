/* eslint-disable class-methods-use-this, no-underscore-dangle, no-use-before-define */
import Collections from "./collections";
import ItemObject from "./item-object";
import Item from "./item";
import { StorageFactory } from "../storage/interfaces";
import { DefaultStorageFactory } from "../storage/storage-factory";
import { debugLog } from "../debug";

export default class Database extends Item {
  collections: Collections;

  constructor(data: ItemObject | undefined | null, storageFactory?: StorageFactory) {
    super(data);
    
    // Set the database context in the storage factory
    if (storageFactory && storageFactory instanceof DefaultStorageFactory && data?._rid) {
      storageFactory.setCurrentDatabase(data._rid);
      debugLog(`Set database context to: ${data._rid}`);
    }
    
    this.collections = new Collections(this, ["/id"], [], storageFactory);
  }

  collection(idOrRid: string) {
    return this.collections._item("/id", idOrRid, idOrRid);
  }
}

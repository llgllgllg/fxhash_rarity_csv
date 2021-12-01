import json
import os
import codecs
import requests
import pandas as pd
import numpy as np
from scipy.stats import rankdata
from tqdm import tqdm

def dump_json(x, path):
  with open(path, "w") as f:
    json.dump(x, f, indent=4)

def load_json(path):
  with open(path, "r") as f:
    return json.load(f)

def get(url):
  return requests.get("https://api.tzkt.io/v1/" + url).json()

def get_bigmap(contract, bigmap):
  return get("contracts/{}/bigmaps/{}".format(contract,bigmap))

def get_bigmap_keys(contract, bigmap, single=False):
  data = get_bigmap(contract, bigmap)
  print("THERE ARE {} KEYS".format(data["totalKeys"]))
  keys = []
  for offset in tqdm(range(0,data["totalKeys"],10000),leave=False):
    keys.extend( get("contracts/{}/bigmaps/{}/keys?limit=10000&offset={}".format(contract,bigmap,offset)) )
    if single:
      break
  return keys

def get_token_map(project_id):
  path = "tokens_{}.json".format(project_id)
  if not os.path.exists(path):
    token_data = get_bigmap_keys(
      "KT1AEVuykWeuuFX7QkEAMNtffzwhe1Z98hJS", "ledger_gentk")
    mm = {}
    for item in token_data:
      if int(item["value"]["issuer_id"]) == project_id:
        mm[ item["value"]["iteration"] ] = item["key"]
    dump_json(mm, path)
  return {int(k):int(v) for k,v in load_json(path).items()}

def get_metadata_urls(project_id):
  path = "metadata_urls_{}.json".format(project_id)
  if not os.path.exists(path):
    tokens = set(list(get_token_map(project_id).values()))
    keys = get_bigmap_keys(
      "KT1KEa8z6vWXDJrVqtMrAeDVzsvxat3kHaCE", "token_metadata")
    metadata_map = {}
    for item in keys:
      if int(item["value"]["token_id"]) in tokens:
        metadata_map[int(item["value"]["token_id"])] = codecs.decode(item["value"]["token_info"][""], "hex").decode("utf-8").split("/")[-1]
    dump_json(metadata_map, path)
  return {int(k):v for k,v in load_json(path).items()}

def get_metadata(project_id):
  path = "metadata_{}.json".format(project_id)
  if not os.path.exists(path):
    meta = {}
    metadata_urls = get_metadata_urls(project_id)
    for k,v in tqdm(list(metadata_urls.items())):
      meta[k] = requests.get("https://gateway.fxhash.xyz/ipfs/" + v).json()
    dump_json(meta, path)
  return {int(k):v for k,v in load_json(path).items()}

def make_rarity_csv(project_id):
  meta = get_metadata(project_id)

  attributes = [a["name"] for a in meta[list(meta.keys())[0]]["attributes"]]
  other_keys = ["iteration", "gentk_token_id", "thumbnail", "display"]

  storage = {k : [] for k in other_keys + attributes}
  for token_id,item in meta.items():
    for attr in item["attributes"]:
      storage[attr["name"]].append( attr["value"] )
    iteration = int(item["name"].split("#")[-1])
    storage["iteration"].append( iteration )
    storage["thumbnail"].append( item["thumbnailUri"] )
    storage["display"].append( item["displayUri"] )
    storage["gentk_token_id"].append( token_id )
    
  df = pd.DataFrame()
  for k,v in storage.items():
    df[k] = v
  
  #calculate rarity
  scores = []
  for _,row in df.iterrows():
    scores.append( np.sum(1. / np.array([np.sum(df[k] ==row[k]) for k in attributes])) )
  
  scores = np.array(scores)
  
  df["RARITY SCORE"] = scores
  df["RARITY RANK"] = rankdata(np.max(scores) - scores, method="dense")

  df.to_csv("rarity_{}.csv".format(project_id), index=False)

if __name__ == "__main__":

  import argparse
  parser = argparse.ArgumentParser()
  parser.add_argument('--collection_id', type=int, required=True)
  args = parser.parse_args()

  make_rarity_csv(args.collection_id)
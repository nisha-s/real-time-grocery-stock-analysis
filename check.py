import os
from ultralytics import YOLO

_BASE = os.path.dirname(os.path.abspath(__file__))
model = YOLO(os.path.join(_BASE, "runs", "classify", "grozi_cls_v8n_aug10", "weights", "best.pt"))
print(model.names)

train_root = os.path.join(_BASE, "grozi_classification", "train")
classes = sorted(os.listdir(train_root), key=lambda x: int(x))
print(classes[:20]) 
print("Total classes:", len(classes))

import h5py

def explore_h5_file(file_path):
    # 打开H5文件
    with h5py.File(file_path, 'r') as h5_file:
        print(f"文件名: {file_path}")
        print(f"根组: {h5_file}")
        print(f"根组中的键: {list(h5_file.keys())}")
        
        # 递归遍历H5文件中的所有组和数据集
        def explore_group(group, indent=0):
            for key in group.keys():
                item = group[key]
                if isinstance(item, h5py.Dataset):
                    # 如果是数据集，打印其信息
                    print(f"{' ' * indent}数据集: {key}")
                    print(f"{' ' * indent}  形状: {item.shape}")
                    print(f"{' ' * indent}  数据类型: {item.dtype}")
                    print(f"{' ' * indent}  前10个数据: {item[:10]}")
                elif isinstance(item, h5py.Group):
                    # 如果是组，递归探索
                    print(f"{' ' * indent}组: {key}")
                    explore_group(item, indent + 4)
                else:
                    print(f"{' ' * indent}未知类型: {key}")
        
        explore_group(h5_file)

# 使用示例
file_path = r"C:/Users/16532/Downloads/md_20221226.h5"
explore_h5_file(file_path)


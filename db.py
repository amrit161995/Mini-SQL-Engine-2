import pprint
import sqlparse
import sys
# creating table structure
table_list_name = []
table_list = {}
aggregate_list = [ 'max(' , 'min(' , 'sum(' ,'average(']
with open("metadata.txt") as f:
    content = f.read().splitlines()
flag=0
for i in range(len(content)):
	if(content[i-1] == '<begin_table>'):
		exec("%s = %s" % (content[i], {}))
		flag = 1
		current_dict = content[i]
		continue
	if content[i] == '<end_table>':
		flag = 0
		exec("table_list['%s']= %s " % (current_dict,current_dict))
		table_list_name.append(current_dict)
		# table_list.append(current_dict)
	if flag == 1:
		exec("%s['%s'] = %s" % (current_dict,content[i], []))
		#print(current_dict)

# populating the tables

for i in table_list_name:
	table_file = i + '.csv'
	with open(table_file) as tf:
		content =tf.read().splitlines()
		for row in content:
			split_row = row.split(',')
			exec("table_name = %s" % (i))
			for k in range(len(table_name.keys())):
				list_keys = (list(table_name.keys()))
				table_name[list_keys[k]].append(int(split_row[k]))
			# print(table_name)
				# print (values)
# print(table_list['table2'])

# print(table2)
# for key,values in table1.items():
# 	print(values[0],)
def print_result(column_list,all_list):
	if(len(column_list)==0):
		print('Invalid Input')
		sys.exit()
	for i in range(len(column_list)):# names of columns printed by this for loop
		if(i == len(column_list)-1 ):
			print('%s'%(column_list[i]),end ="")
		else:
			print('%s,'%(column_list[i]),end ="")
	print()

	for row in all_list: # rows
		for i in range(len(row)):
			if(i ==  len(row)-1):
				print('%s'%row[i],end ="")
			else:
				print('%s,'%row[i],end ="")
		print()

def select_query(columns,tables):
	tab=tables[0]
	current_table = table_list[tab]
	number_of_rows=0

	if columns == '*':
		for i in range(len(current_table.keys())):  #printing column names
			number_of_rows = len(current_table[list(current_table.keys())[i]])
			if(i == len(current_table.keys())-1 ): # printing rows
				print('%s.%s'%(tab,list(current_table.keys())[i]),end ="")
			else:
				print('%s.%s,'%(tab,list(current_table.keys())[i]),end ="")
		print()
		for i in range(number_of_rows):
			for j in range(len(current_table.keys())):
				if(j ==  len(current_table.keys())-1):
					print('%s'%current_table[list(current_table.keys())[j]][i],end ="")
				else:
					print('%s,'%current_table[list(current_table.keys())[j]][i],end ="")
			print()
	
	else:
		for i in range(len(columns)): # printing column names
			number_of_rows = len(current_table[columns[i]])
			if(i == len(columns)-1 ):
				print('%s.%s'%(tab,columns[i]),end ="")
			else:
				print('%s.%s,'%(tab,columns[i]),end ="")
		print()
		for i in range(number_of_rows):# printing rows
			for j in range(len(columns)):
				if(j ==  len(columns)-1):
					print('%s'%current_table[columns[j]][i],end ="")
				else:
					print('%s,'%current_table[columns[j]][i],end ="")
			print()

def aggregate(i,table_name,column_name):

	if i == 0 :
		print('max('+table_name+'.'+column_name+')')
		print(max(table_list[table_name][column_name]))
	elif i == 1 :
		print('min('+table_name+'.'+column_name+')')
		print(min(table_list[table_name][column_name]))
	elif i == 2 :
		print('sum('+table_name+'.'+column_name+')')
		print(sum(table_list[table_name][column_name]))
	elif i == 3 :
		print('average('+table_name+'.'+column_name+')')
		print(sum(table_list[table_name][column_name])/len(table_list[table_name][column_name]))
	# elif function_name == 'min':
# print(table_list)

def cartesian(total_list):
	product = []
	complete_list = [e for e in total_list if e]
	for i in range(len(complete_list)):
		if(i == 0):
			product = complete_list[i]
		else:
			temp = []
			for j in range(len(product)):
				for k in complete_list[i]:
					temp.append(product[j] + k)
			product = temp
	return product
	
def select_multiple(columns,tables):
	column_list = []
	total_list = []
	for tab in tables:
		current_table = table_list[tab]
		number_of_rows=0
		if columns[0] == '*': 
			for i in range(len(current_table.keys())): # names of columns printed by this for loop
				number_of_rows = len(current_table[list(current_table.keys())[i]])
				column_list.append(tab+'.'+list(current_table.keys())[i])
				
			all_list = []
			for i in range(number_of_rows):
				tup = ()
				for j in range(len(current_table.keys())):
					tup+=(current_table[list(current_table.keys())[j]][i],)
				all_list.append(tup)
			total_list.append(all_list)

	
		else: # columns given
			for i in range(len(columns)):# names of columns printed by this for loop
				if columns[i] in current_table.keys():
					number_of_rows = len(current_table[columns[i]])
					column_list.append(tab+'.'+columns[i])
			all_list = []
			for i in range(number_of_rows):
				tup = ()
				
				for j in range(len(columns)):
					if columns[j] in current_table.keys():
						tup+=(current_table[columns[j]][i],)
				all_list.append(tup)
			total_list.append(all_list)
	product=cartesian(total_list)
	print_result(column_list,product)	
		
			# print_result(columns,all_list)

def select_distinct(columns,tables):
	tab=tables[0]
	current_table = table_list[tab]
	number_of_rows=0
	column_list=[]
	distinct_list=[]
	if columns == '*':
		for i in range(len(current_table.keys())):
			number_of_rows = len(current_table[list(current_table.keys())[i]])
			column_list.append(tab+'.'+list(current_table.keys())[i])
			
		all_list = []
		for i in range(number_of_rows):
			tup = ()
			for j in range(len(current_table.keys())):
				tup+=(current_table[list(current_table.keys())[j]][i],)
			all_list.append(tup)
		
		for distinct in all_list:
			   if distinct not in distinct_list:
			       distinct_list.append(distinct)
			

	
	else:
		for i in range(len(columns)):
			number_of_rows = len(current_table[columns[i]])
			column_list.append(tab+'.'+columns[i])
		
		all_list = []
		for i in range(number_of_rows):
			tup = ()
			for j in range(len(columns)):
				tup+=(current_table[list(columns)[j]][i],)
			all_list.append(tup)
		
		for distinct in all_list:
			   if distinct not in distinct_list:
			       distinct_list.append(distinct)

	print_result(column_list,distinct_list)

def select_where(columns,tables,keys,operators,values,logic=None):
	tab=tables[0]
	current_table = table_list[tab]
	number_of_rows=0
	column_list = []

	if columns == '*': 
		for i in range(len(current_table.keys())): # names of columns printed by this for loop
			number_of_rows = len(current_table[list(current_table.keys())[i]])
			column_list.append(tab+'.'+list(current_table.keys())[i])
		
		all_list = []
		for i in range(number_of_rows):
			tup = ()
			flag=0
			if(len(keys) == 1):
				if not eval("%s%s%s" % (current_table[keys[0]][i],operators[0],values[0])):
					continue
			elif(len(keys) == 2):
				if not eval("%s%s%s %s %s%s%s" % (current_table[keys[0]][i],operators[0],values[0],logic,current_table[keys[1]][i],operators[1],values[1])):
					continue
			for j in range(len(current_table.keys())):
				tup+=(current_table[list(current_table.keys())[j]][i],)
			all_list.append(tup)
		
		# print_result(column_list,all_list)
	else: # columns given
		for i in range(len(columns)):# names of columns printed by this for loop
			number_of_rows = len(current_table[columns[i]])
			column_list.append(tab+'.'+columns[i])
		all_list = []
		for i in range(number_of_rows):
			tup = ()
			flag=0
			if(len(keys) == 1):
				if not eval("%s%s%s" % (current_table[keys[0]][i],operators[0],values[0])):
					continue
			elif(len(keys) == 2):
				if not eval("%s%s%s %s %s%s%s" % (current_table[keys[0]][i],operators[0],values[0],logic,current_table[keys[1]][i],operators[1],values[1])):
					continue
			for j in range(len(columns)):
				tup+=(current_table[list(columns)[j]][i],)
			all_list.append(tup)
		
		
	print_result(column_list,all_list)


def select_join(columns,tables,join):
	column_list = []
	total_list = []
	for tab in tables:
		current_table = table_list[tab]
		number_of_rows=0
		if columns[0] == '*': 
			for i in range(len(current_table.keys())): # names of columns printed by this for loop
				number_of_rows = len(current_table[list(current_table.keys())[i]])
				column_list.append(tab+'.'+list(current_table.keys())[i])
				
			all_list = []
			for i in range(number_of_rows):
				tup = ()
				for j in range(len(current_table.keys())):
					tup+=(current_table[list(current_table.keys())[j]][i],)
				all_list.append(tup)
			total_list.append(all_list)

	
		else: # columns given
			for i in range(len(columns)):# names of columns printed by this for loop
				if columns[i] in current_table.keys():
					number_of_rows = len(current_table[columns[i]])
					column_list.append(tab+'.'+columns[i])
			all_list = []
			for i in range(number_of_rows):
				tup = ()
				
				for j in range(len(columns)):
					if columns[j] in current_table.keys():
						tup+=(current_table[columns[j]][i],)
				all_list.append(tup)
			total_list.append(all_list)
	product=cartesian(total_list)	
	index1 = 0
	index2 = 0
	for i in range(len(column_list)):
		if(join[0] == column_list[i]):
			index1 = i
		if(join[1] == column_list[i]):
			index2 = i
	new_product=[]		
	for j in range(len(product)):
		if(product[j][index1] == product[j][index2]):
			new_product.append(product[j])
	print_result(column_list,new_product)	


def select_wh(columns,tables,keys,operators,values,logic=None):
	column_list = []
	total_list = []
	for tab in tables:
		current_table = table_list[tab]
		number_of_rows=0
		if columns[0] == '*': 
			for i in range(len(current_table.keys())): # names of columns printed by this for loop
				number_of_rows = len(current_table[list(current_table.keys())[i]])
				column_list.append(tab+'.'+list(current_table.keys())[i])
				
			all_list = []
			for i in range(number_of_rows):
				tup = ()
				for j in range(len(current_table.keys())):
					tup+=(current_table[list(current_table.keys())[j]][i],)
				all_list.append(tup)
			total_list.append(all_list)

	
		else: # columns given
			for i in range(len(columns)):# names of columns printed by this for loop
				if columns[i] in current_table.keys():
					number_of_rows = len(current_table[columns[i]])
					column_list.append(tab+'.'+columns[i])
			all_list = []
			for i in range(number_of_rows):
				tup = ()
				
				for j in range(len(columns)):
					if columns[j] in current_table.keys():
						tup+=(current_table[columns[j]][i],)
				all_list.append(tup)
			total_list.append(all_list)
	product=cartesian(total_list)	
	index1 = 0
	index2 = 0
	for i in range(len(column_list)):
		if(join[0] == column_list[i]):
			index1 = i
		if(join[1] == column_list[i]):
			index2 = i
	new_product=[]		
	for j in range(len(product)):
		if(product[j][index1] == product[j][index2]):
			new_product.append(product[j])
	print_result(column_list,new_product)	

# select_query(["A","B"],["table1"])
# print(max(table_list["table1"]['B']))
# select_where(["A",'C'],["table1"],['A','B'],['==','=='],[7,731],'or')

def parse_query(sql):
	parsed = sqlparse.parse(sql)
	stmt = parsed[0]
	str(stmt)
	return (stmt.tokens)

def main():
	sql=sys.argv
	if ( len(sql) == 2):
		sql=str(sql[1])
		tokens = parse_query(sql)
		# print (tokens[-3])	
		if(str(tokens[0]).lower() == 'select'):
			if (str(tokens[2]).lower() == 'distinct'):
				column_string = str(tokens[4]).replace(" ", "")
				columns = column_string.split(',')
				if (str(tokens[-1])[0:5] != 'where'):
					table_string = str(tokens[-1]).replace(" ", "")
					table_list = table_string.split(',')
					select_distinct(columns,table_list)
					sys.exit()
			else:
				column_string = str(tokens[2]).replace(" ", "")
				columns = column_string.split(',')
				if (str(tokens[-1])[0:5] != 'where'):
					table_string = str(tokens[-1]).replace(" ", "")
					table_list = table_string.split(',')
					s=columns[0]
					
					for i in range(len(aggregate_list)):
						if aggregate_list[i] in s:
							aggregate(i,table_list[0],s[s.find("(")+1:s.find(")")])
							sys.exit()
					select_multiple(columns,table_list)
					sys.exit()

	else:
		print('Invalid Input')	

main()

# select_join(['A','B','C','D'],["table1","table2"],["table1.B","table2.B"])
# select_distinct(["A",'B'],["table1"])
# aggregate('min',"table1","A")